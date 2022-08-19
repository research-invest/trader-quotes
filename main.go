package main

//GOOS=linux GOARCH=amd64 go build -o ./quotes -a

//https://api.binance.com/api/v3/klines?interval=1m&limit=20&symbol=AVAXBUSD

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/adshao/go-binance/v2"
	"github.com/cheggaaa/pb/v3"
	"github.com/go-pg/pg/extra/pgdebug"
	"github.com/go-pg/pg/v10"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/olekukonko/tablewriter"
	"github.com/sirupsen/logrus"
	"github.com/wcharczuk/go-chart"
	"github.com/wcharczuk/go-chart/drawing"
	"go-trader/models"
	"os"
	"strconv"
	"strings"
	"time"
)

var dbConnect pg.DB
var log = logrus.New()
var getKlinesIsWorking, getAccountsInfoIsWorking, getOrdersAccountsIsWorking bool

func main() {
	setLogParam()
	readConfig()

	dbInit()

	redisInit()

	defer func() {
		err := dbConnect.Close()
		if err != nil {
			fmt.Printf("Error close postgres connection: %v\n", err.Error())
			log.Fatalf("dbConnect.Close fatal error : %v", err)
		}
	}()

	go func() {
		telegramBot()
	}()

	go func() {
		for {
			getAccountsInfo()
			getOrdersAccounts()
			sendNotificationsAccounts()
			time.Sleep(30 * time.Minute)
		}
	}()

	testSendImages()


	//getAccountsInfo()

	for {
		t := time.Now()

		if t.Hour() >= 3 && t.Hour() < 6 {
			time.Sleep(1 * time.Hour) // temp
		}

		if t.Hour() == 0 && t.Minute() == 0 {
			CounterQueriesApiSetZero()
		}

		if t.Second() == 0 {
			getKlines()
			time.Sleep(1 * time.Minute)
		}

	}
}

//func wsTest() {
//
//	wsDepthHandler := func(event *binance.WsDepthEvent) {
//		jsonF, _ := json.Marshal(event)
//		fmt.Println(string(jsonF))
//		fmt.Println("string(jsonF))")
//	}
//	errHandler := func(err error) {
//		fmt.Println(err)
//	}
//	doneC, stopC, err := binance.WsDepthServe("BTCBUSD", wsDepthHandler, errHandler)
//	if err != nil {
//		fmt.Println(err)
//		return
//	}
//	// use stopC to exit
//	go func() {
//		time.Sleep(5 * time.Second)
//		stopC <- struct{}{}
//	}()
//	// remove this if you do not want to be blocked here
//	<-doneC
//
//	//wsAggTradeHandler := func(event *binance.WsAggTradeEvent) {
//	//	jsonF, _ := json.Marshal(event)
//	//	fmt.Println(string(jsonF))
//	//}
//	//errHandler := func(err error) {
//	//	fmt.Println(err)
//	//}
//	//doneC, _, err := binance.WsAggTradeServe("LTCBTC", wsAggTradeHandler, errHandler)
//	//if err != nil {
//	//	fmt.Println(err)
//	//	return
//	//}
//	//<-doneC
//
//	return
//	//wsKlineHandler := func(event *binance.WsKlineEvent) {
//	//jsonF, _ := json.Marshal(event)
//	//fmt.Println(string(jsonF))
//	//}
//	//errHandler := func(err error) {
//	//	fmt.Println(err)
//	//}
//	//doneC, _, err := binance.WsKlineServe("BTCBUSD", "1m", wsKlineHandler, errHandler)
//	//if err != nil {
//	//	fmt.Println(err)
//	//	return
//	//}
//	//<-doneC
//}

func dbInit() {
	dbConnect = *pg.Connect(&pg.Options{
		Addr:     appConfig.Db.Host + ":" + strconv.Itoa(appConfig.Db.Port),
		User:     appConfig.Db.User,
		Password: appConfig.Db.Pass,
		Database: appConfig.Db.Dbname,
	})

	ctx := context.Background()

	_, err := dbConnect.ExecContext(ctx, "SELECT 1; SET timezone = 'UTC';")
	if err != nil {
		log.Warn(err)
		panic(err)
	}

	dbConnect.AddQueryHook(pgdebug.DebugHook{
		// Print all queries.
		Verbose: false,
	})
}

func setLogParam() {
	log.Out = os.Stdout

	file, err := os.OpenFile("./logs/logrus.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err == nil {
		log.Out = file
	} else {
		log.Info("Failed to log to file, using default stderr")
	}
}

func telegramBot() {
	bot, err := tgbotapi.NewBotAPI(appConfig.TelegramBot)
	if err != nil {
		log.Panic(err)
	}

	var replyMarkup = tgbotapi.NewReplyKeyboard(
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("Баланс"),
			tgbotapi.NewKeyboardButton("Средняя цена"),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("Статус"),
			tgbotapi.NewKeyboardButton("Получить кол-во апи запросов"),
			tgbotapi.NewKeyboardButton("Кол-во свечей 🕯"),
			tgbotapi.NewKeyboardButton("Синхронизировать балансы"),
		),
	)

	bot.Debug = false

	log.Printf("Authorized on account %s", bot.Self.UserName)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updates := bot.GetUpdatesChan(u)

	for update := range updates {
		if update.Message == nil { // ignore any non-Message updates
			continue
		}

		acc := Account{}
		account, err := acc.addNew(update.Message.Chat)

		if err != nil {
			fmt.Printf("can't add a new file db record : %v\n", err)
			log.Warnf("can't account create : %v", err)
			continue
		}

		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "")
		msg.ParseMode = "MarkdownV2"
		msg.ReplyMarkup = replyMarkup

		//if update.Message.IsCommand() { // ignore any non-command Messages

		/*
			getbalance - Get balance info
			syncbalance - Sync balance info
			getavgprices - Get avg prices coins
			getcountqueriesapi - Get count queries api
			getcountqueriesapierror - Get count queries api error
			getcountklines - Get count klines
			status - Status service
			setapikey - Set binance api key read only
			setsecretkey - Set binance secret key read only
		*/

		// Extract the command from the Message.
		switch update.Message.Text {
		case "setapikey":
			apiKey := strings.Replace(update.Message.Text, "/setapikey", "", 1)
			err := account.saveApiKey(apiKey)
			if err != nil {
				msg.Text = "No correct api key"
				log.Warnf("can't save api key account: %v", err)
				//break
			} else {
				msg.Text = "Api key saved."
				//break
			}
		case "setsecretkey":
			secretKey := strings.Replace(update.Message.Text, "/setsecretkey", "", 1)
			err := account.saveSecretKey(secretKey)
			if err != nil {
				msg.Text = "No correct secret key"
				log.Warnf("can't save secret key account: %v", err)
				//break
			} else {
				msg.Text = "Api secret key saved."
				//break
			}

		case "Получить кол-во апи запросов":
			msg.Text = "Count query api: " + getCountQueriesApi()
		case "getcountqueriesapierror":
			msg.Text = "Count query api errors: " + getCountQueriesApiError()
		case "Кол-во свечей 🕯":
			msg.Text = "Кол-во свечей: " + strconv.FormatInt(getCountKlines(), 10)
		case "Статус":
			msg.Text = "Все нормально, 😉 работаем"
		case "Баланс":
			msg.Text = "```" + getBalanceInfo(account.Id) + "```"
		case "Средняя цена":
			msg.Text = "```" + getAvgPrices(account.Id) + "```"
		case "Синхронизировать балансы":
			getAccountsInfo()
			getOrdersAccounts()
			msg.Text = "sync"
		default:
			msg.Text = "I don't know that command"
		}

		if _, err := bot.Send(msg); err != nil {
			log.Warnf("can't send bot message: %v", err)
		}

		continue
		//}

		rate, err := getActualExchangeRate(update.Message.Text)

		if err == nil {
			msg.Text = "```" + rate + "```"
			if _, err := bot.Send(msg); err != nil {
				log.Warnf("can't send bot message getActualExchangeRate: %v", err)
			}
		} else {
			msg.Text = err.Error()
			if _, err := bot.Send(msg); err != nil {
				log.Warnf("can't send bot message getActualExchangeRate: %v", err)
			}
		}
	}
}

func getCountKlines() int64 {
	count, _ := dbConnect.Model((*Kline)(nil)).Count()
	return int64(count)
}

func getPairs(pairs *[]Pair) (err error) {
	_, err = dbConnect.Query(pairs, `
	
WITH coin_pairs_close_time AS (
    SELECT DISTINCT ON (k.coin_pair_id) k.coin_pair_id, k.close_time
    FROM klines AS k
    INNER JOIN coins_pairs cp on cp.id = k.coin_pair_id
    INNER JOIN coins c on c.id = cp.coin_id
    WHERE cp.is_enabled = 1 AND c.is_enabled = 1 AND k.close_time >= NOW() - INTERVAL '30 DAYS'
    ORDER BY k.coin_pair_id, k.close_time DESC
)

SELECT t.pair, t.coin_id, t.coin_pair_id, t.interval
FROM (
         SELECT (c.code || cp.couple) AS pair,
                c.id                  AS coin_id,
                cp.id                 AS coin_pair_id,
                cpct.close_time     AS close_time,
                c.interval,
                c.rank,
                CASE
                    WHEN (c.interval = '1m') THEN NOW() - INTERVAL '1 MINUTES'
                    WHEN (c.interval = '3m') THEN NOW() - INTERVAL '3 MINUTES'
                    WHEN (c.interval = '5m') THEN NOW() - INTERVAL '5 MINUTES'
                    WHEN (c.interval = '15m') THEN NOW() - INTERVAL '15 MINUTES'
                    WHEN (c.interval = '30m') THEN NOW() - INTERVAL '30 MINUTES'
                    WHEN (c.interval = '1h') THEN NOW() - INTERVAL '1 HOURS'
                    WHEN (c.interval = '2h') THEN NOW() - INTERVAL '2 HOURS'
                    WHEN (c.interval = '4h') THEN NOW() - INTERVAL '4 HOURS'
                    WHEN (c.interval = '6h') THEN NOW() - INTERVAL '6 HOURS'
                    WHEN (c.interval = '8h') THEN NOW() - INTERVAL '8 HOURS'
                    WHEN (c.interval = '12h') THEN NOW() - INTERVAL '12 HOURS'
                    WHEN (c.interval = '1d') THEN NOW() - INTERVAL '1 DAYS'
                    WHEN (c.interval = '3d') THEN NOW() - INTERVAL '3 DAYS'
                    WHEN (c.interval = '1w') THEN NOW() - INTERVAL '7 DAYS'
                    WHEN (c.interval = '1M') THEN NOW() - INTERVAL '30 DAYS'
                    END AS time_interval
         FROM coins_pairs AS cp
                  INNER JOIN coins c on c.id = cp.coin_id
                  LEFT JOIN coin_pairs_close_time AS cpct ON cpct.coin_pair_id = cp.id
         WHERE cp.is_enabled = 1 AND c.is_enabled = 1
         GROUP BY pair, c.id, cp.id, c.interval, cpct.close_time, c.rank
     ) AS t
WHERE (t.close_time IS NULL OR t.close_time < t.time_interval)
ORDER BY t.rank;
`)

	if err != nil {
		log.Warn("can't get pairs: %v", err)
		return err
	}

	return nil
}

func getKlines() {

	if getKlinesIsWorking == true {
		return
	}

	fmt.Println("Get klines start work")

	var pairs []Pair
	err := getPairs(&pairs)

	if err != nil {
		return
	}

	countPairs := len(pairs)

	if countPairs == 0 {
		fmt.Println("countPairs is zero")
		getKlinesIsWorking = false
		return
	}

	getKlinesIsWorking = true

	client := binance.NewClient(appConfig.BinanceApiKeyReadOnly, appConfig.BinanceSecretKeyReadOnly)

	// create and start new bar
	bar := pb.StartNew(countPairs)

	for _, pair := range pairs {

		klines, err := client.NewKlinesService().Symbol(strings.ToUpper(pair.Pair)).
			Interval(pair.Interval).Do(context.Background())

		CounterQueriesApiIncr()

		//if pair.CoinPairId == 3 {
		//	fmt.Println("klines")
		//	jsonF, _ := json.Marshal(klines)
		//	fmt.Println(string(jsonF))
		//}

		if err != nil {
			CounterQueriesApiIncrError()

			fmt.Println(err.Error())
			log.Warnf("get NewKlinesService error: %v", err)
			continue
		}

		for _, kline := range klines {
			volume, _ := strconv.ParseFloat(kline.Volume, 64)

			if volume == 0 {
				continue
			}

			open, _ := strconv.ParseFloat(kline.Open, 64)
			high, _ := strconv.ParseFloat(kline.High, 64)
			low, _ := strconv.ParseFloat(kline.Low, 64)
			closeKline, _ := strconv.ParseFloat(kline.Close, 64)
			quoteAssetVolume, _ := strconv.ParseFloat(kline.QuoteAssetVolume, 64)
			takerBuyBaseAssetVolume, _ := strconv.ParseFloat(kline.TakerBuyBaseAssetVolume, 64)
			takerBuyQuoteAssetVolume, _ := strconv.ParseFloat(kline.TakerBuyQuoteAssetVolume, 64)

			newKline := &Kline{
				CoinPairId:               pair.CoinPairId,
				OpenTime:                 getTimestampFromMilliseconds(kline.OpenTime),
				CloseTime:                getTimestampFromMilliseconds(kline.CloseTime - 1000),
				Open:                     open,
				High:                     high,
				Low:                      low,
				Close:                    closeKline,
				Volume:                   volume,
				QuoteAssetVolume:         quoteAssetVolume,
				TradeNum:                 kline.TradeNum,
				TakerBuyBaseAssetVolume:  takerBuyBaseAssetVolume,
				TakerBuyQuoteAssetVolume: takerBuyQuoteAssetVolume,
				RatioOpenClose:           open / closeKline,
				RatioHighLow:             high / low,
			}

			count, err := dbConnect.Model(newKline).
				Where("coin_pair_id = ?coin_pair_id AND open_time = ?open_time").
				Count()

			if count == 0 {
				_, err = dbConnect.Model(newKline).Insert()
			} else {
				_, err = dbConnect.Model(newKline).
					Where("coin_pair_id = ?coin_pair_id AND open_time = ?open_time").
					Update()
			}

			if err != nil {
				log.Warnf("add newKline error: %v, cpid: %v, open_time: %v\n", err.Error(), newKline.CoinPairId, newKline.OpenTime)
				fmt.Printf("add newKline error: %v, cpid: %v, open_time: %v\n", err.Error(), newKline.CoinPairId, newKline.OpenTime)
				if !strings.Contains(err.Error(), "ERROR #23505 duplicate key value violates unique constraint") {
					fmt.Printf("can't add a new file db record : %v\n", err.Error())
				}
			}
		}

		bar.Increment()
	}

	bar.Finish()

	getKlinesIsWorking = false
}

func getAccountsInfo() {
	if getAccountsInfoIsWorking == true {
		return
	}

	getAccountsInfoIsWorking = true

	fmt.Println("Get accounts info start work")

	var accounts []Account
	err := dbConnect.Model(&accounts).
		Where("is_enabled = ?", 1).
		Where("binance_api_key IS NOT NULL AND binance_secret_key IS NOT NULL").
		Select()

	if err != nil {
		log.Warnf("can't get accounts: %v", err)
		panic(err)
	}

	for _, account := range accounts {
		client := binance.NewClient(account.BinanceApiKey, account.BinanceSecretKey)
		accountInfo, err := client.NewGetAccountService().Do(context.Background())

		CounterQueriesApiIncr()

		if err != nil {
			//jsonF, _ := json.Marshal(err)
			//fmt.Println(string(jsonF))
			//{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}
			CounterQueriesApiIncrError()

			if strings.Contains(err.Error(), "<APIError> code=-2015, msg=Invalid API-key, IP, or permissions for action.") { // to const error text
				err := account.disableAccount()
				if err != nil {
					fmt.Println(err.Error())
					log.Warnf("Error disable account: %v", err)
					return
				}
			} else {
				fmt.Println(err.Error())
				log.Warnf("can't get accountInfo NewGetAccountService: %v", err)
			}

			continue
		}

		balanceCoins := account.getCoinsInBalance()

		var locked, free float64

		for _, balance := range accountInfo.Balances {
			if locked, err = strconv.ParseFloat(balance.Locked, 64); err != nil {
				fmt.Printf("Error parse fload locked balance : %v\n", err.Error())
				log.Warnf("Error parse fload locked balance: %v", err)
				continue
			}

			if free, err = strconv.ParseFloat(balance.Free, 64); err != nil {
				fmt.Printf("Error parse fload free balance: %v\n", err.Error())
				log.Warnf("Error parse fload free balance: %v", err)
				continue
			}

			if locked == 0 && free == 0 {
				isBalanceCoin := false
				for _, coin := range balanceCoins {
					if coin.Code == balance.Asset {
						isBalanceCoin = true
						break
					}
				}

				if isBalanceCoin == false {
					continue
				}
			}

			coin := new(Coin)
			err = dbConnect.Model(coin).
				Where("code = ?", balance.Asset). //,  AND is_enabled = ? IS_ENABLED_TRUE
				First()

			if err != nil {
				fmt.Printf("Error get coin by name balance: %v coin: %v\n", err.Error(), balance.Asset)
				log.Warnf("Error get coin by name balance: %v coin: %v", err, balance.Asset)
				continue
			}

			newBalance := &Balance{
				CoinId:    coin.Id,
				AccountId: account.Id,
				Free:      free,
				Locked:    locked,
				CreatedAt: time.Now(),
			}

			_, err := dbConnect.Model(newBalance).
				Where("coin_id = ?coin_id AND account_id = ?account_id AND free = ?free AND locked = ?locked").
				OnConflict("DO NOTHING").
				SelectOrInsert()
			//Insert()

			if err != nil {
				fmt.Printf("add new balance error: %v\n", err.Error())
				log.Warnf("add new balance error: %v", err.Error())
			}

		}

		getAccountsInfoIsWorking = false
	}

	//if pair.CoinPairId == 3 {
	//	fmt.Println("klines")
	//	jsonF, _ := json.Marshal(klines)
	//	fmt.Println(string(jsonF))
	//}

	if err != nil {
		fmt.Println(err.Error())
		return
	}

}

func getTimestampFromMilliseconds(milliseconds int64) time.Time {
	return time.Unix(0, milliseconds*int64(time.Millisecond))
}

func getBalanceInfo(accountId int64) string {
	var balance []BalanceInfo
	res, err := dbConnect.Query(&balance, `
	
WITH coins_last_prices AS (
    SELECT DISTINCT ON (k.coin_pair_id) k.coin_pair_id,
                                        c.id,
                                        c.code,
                                        c.rank,
                                        k.low,
                                        k.high
    FROM klines AS k
             INNER JOIN coins_pairs AS cp ON cp.id = k.coin_pair_id
             INNER JOIN coins AS c ON c.id = cp.coin_id
    WHERE cp.coin_id IN (
        SELECT DISTINCT ON (coin_id) coin_id FROM balances WHERE account_id = ?
    ) AND cp.couple = 'BUSD'
      AND c.is_enabled = 1 AND cp.is_enabled = 1
      AND k.close_time >= NOW() - INTERVAL '1 DAY'
    ORDER BY k.coin_pair_id, k.close_time DESC
), bal AS (
    SELECT DISTINCT ON (b.coin_id) b.coin_id,
                                   b.free,
                                   b.locked
    FROM balances AS b
    WHERE b.coin_id IN (
		SELECT t.coin_id FROM (
			SELECT DISTINCT ON (b.coin_id) b.coin_id, b.free, b.locked
			FROM balances AS b
			WHERE account_id = ?
			ORDER BY b.coin_id, b.created_at DESC
		) AS t
		WHERE t.free > 0 OR t.locked > 0
    )
    ORDER BY b.coin_id, b.created_at DESC
)

SELECT clp.code, clp.rank,
       ROUND(CAST((b.free +b.locked) AS NUMERIC), 2) AS quantity,
       ROUND(CAST(clp.high AS NUMERIC), 2)  AS price,
       ROUND(CAST(((b.free +b.locked) * clp.high) AS NUMERIC), 2)  AS sum
FROM bal AS b
INNER JOIN coins_last_prices AS clp ON clp.id = b.coin_id
UNION
SELECT 'BUSD', 0,
       ROUND(CAST((b.free +b.locked) AS NUMERIC), 2) AS quantity,
       1 AS price,
       ROUND(CAST(((b.free +b.locked) * 1) AS NUMERIC), 2)  AS sum
FROM bal AS b
WHERE b.coin_id = (SELECT c.id FROM coins AS c WHERE c.code = 'BUSD')
UNION
SELECT 'USDT', 0,
       ROUND(CAST((b.free +b.locked) AS NUMERIC), 2) AS quantity,
       1 AS price,
       ROUND(CAST(((b.free +b.locked) * 1) AS NUMERIC), 2)  AS sum
FROM bal AS b
WHERE b.coin_id = (SELECT c.id FROM coins AS c WHERE c.code = 'USDT')
ORDER BY sum DESC;
	`, accountId, accountId)

	if err != nil {
		log.Warnf("can't get balance info: %v", err)
		return err.Error()
	}

	if res.RowsAffected() == 0 {
		return "Empty balance!"
	}

	tableString := &strings.Builder{}
	table := tablewriter.NewWriter(tableString)
	table.SetHeader([]string{"Name", "Rank", "Quantity", "Price", "Sum"})

	var total float64
	for _, item := range balance {
		table.Append([]string{
			item.Code,
			IntToStr(item.Rank),
			FloatToStr(item.Quantity),
			FloatToStr(item.Price),
			FloatToStr(item.Sum),
		})

		total = total + item.Sum
	}

	table.SetFooter([]string{"", "", "", "Total", fmt.Sprintf("$%.2f", total)})

	table.Render()

	return tableString.String()
}

func getAvgPrices(accountId int64) string {
	var avgPrices []AvgPrice
	res, err := dbConnect.Query(&avgPrices, `

WITH coins_last_prices AS (
    SELECT DISTINCT ON (k.coin_pair_id) k.coin_pair_id,
                                        c.id,
                                        c.code,
                                        c.rank,
                                        k.low,
                                        k.high
    FROM klines AS k
             INNER JOIN coins_pairs AS cp ON cp.id = k.coin_pair_id
             INNER JOIN coins AS c ON c.id = cp.coin_id
    WHERE cp.coin_id IN (
        SELECT t.coin_id
        FROM (
                 SELECT DISTINCT ON (b.coin_id) b.coin_id, b.free, b.locked
                 FROM balances AS b
                 WHERE account_id = ?
                 ORDER BY b.coin_id, b.created_at DESC
             ) AS t
        WHERE t.free > 0
           OR t.locked > 0
    )
      AND cp.couple = 'BUSD'
      AND c.is_enabled = 1
      AND cp.is_enabled = 1
      AND k.close_time >= NOW() - INTERVAL '4 HOUR'
    ORDER BY k.coin_pair_id, k.close_time DESC
)

SELECT t.id, t.code, clp.high AS price, (buy - sell) / (qty_buy - qty_sell) AS avg_price
FROM (
         SELECT sum(cummulative_quote_qty) FILTER (WHERE side = 1) AS buy,
                sum(cummulative_quote_qty) FILTER (WHERE side = 2) AS sell,
                sum(orig_qty) FILTER (WHERE side = 1)              AS qty_buy,
                sum(orig_qty) FILTER (WHERE side = 2)              AS qty_sell,
                c.id,
                c.code
         FROM orders AS o
                  INNER JOIN coins_pairs cp on cp.id = o.coin_pair_id
                  INNER JOIN coins c on c.id = cp.coin_id
         WHERE status = 3
           AND account_id = 1
           AND c.id IN (
             SELECT t.coin_id
             FROM (
                      SELECT DISTINCT ON (b.coin_id) b.coin_id, b.free, b.locked
                      FROM balances AS b
                      WHERE account_id = ?
                      ORDER BY b.coin_id, b.created_at DESC
                  ) AS t
             WHERE t.free > 0
                OR t.locked > 0
         )
         GROUP BY c.id
     ) AS t
         LEFT JOIN coins_last_prices AS clp ON clp.id = t.id
--ORDER BY created_at ASC
;
	`, accountId, accountId)

	if err != nil {
		log.Warnf("can't get avgPrices info: %v", err)
		return err.Error()
	}

	if res.RowsAffected() == 0 {
		return "Empty avgPrices!"
	}

	tableString := &strings.Builder{}
	table := tablewriter.NewWriter(tableString)
	table.SetHeader([]string{"Id", "Coin", "Price", "AvgPrice"})

	for _, item := range avgPrices {
		table.Append([]string{
			IntToStr(int(item.Id)),
			item.Code,
			FloatToStr(item.Price),
			FloatToStr(item.AvgPrice),
		})
	}

	table.Render()

	return tableString.String()
}

func getOrdersAccounts() {

	if getOrdersAccountsIsWorking == true {
		return
	}

	getOrdersAccountsIsWorking = true

	fmt.Println("Get accounts orders work")

	var accounts []Account
	err := dbConnect.Model(&accounts).
		//Relation("Balances").
		//Relation("Balances.Coin").
		Where("account.is_enabled = ?", 1).
		Where("binance_api_key IS NOT NULL AND binance_secret_key IS NOT NULL").
		Select()

	if err != nil {
		log.Warnf("can't get accounts by get orders: %v", err)
		return
	}

	for _, account := range accounts {

		client := binance.NewClient(account.BinanceApiKey, account.BinanceSecretKey)
		//client.Debug = true

		for _, coin := range account.getCoinsInBalance() {

			orders, err := client.NewListOrdersService().Symbol(coin.Pair).
				//StartTime(startTime). //EndTime(endTime).
				Do(context.Background())

			CounterQueriesApiIncr()

			if err != nil {
				fmt.Println(err.Error())
				CounterQueriesApiIncrError()
				log.Warnf("Error NewListOrdersService: %v", err)
				continue
			}

			for _, order := range orders {

				//fmt.Println("order")
				//jsonF, _ := json.Marshal(order)
				//fmt.Println(string(jsonF))

				stopPrice, _ := strconv.ParseFloat(order.StopPrice, 64)
				origQty, _ := strconv.ParseFloat(order.OrigQuantity, 64)
				executedQty, _ := strconv.ParseFloat(order.ExecutedQuantity, 64)
				cummulativeQuoteQuantity, _ := strconv.ParseFloat(order.CummulativeQuoteQuantity, 64)
				icebergQuantity, _ := strconv.ParseFloat(order.IcebergQuantity, 64)
				origQuoteOrderQuantity, _ := strconv.ParseFloat(order.OrigQuoteOrderQuantity, 64)

				newOrder := &models.Order{
					CoinPairId:          coin.PairId,
					AccountId:           account.Id,
					OrderId:             order.OrderID,
					OrderListId:         order.OrderListId,
					ClientOrderId:       order.ClientOrderID,
					OrigQty:             origQty,
					ExecutedQty:         executedQty,
					CummulativeQuoteQty: cummulativeQuoteQuantity,
					StopPrice:           stopPrice,
					IcebergQty:          icebergQuantity,
					OrigQuoteOrderQty:   origQuoteOrderQuantity,
					Time:                order.Time,
					UpdateTime:          order.UpdateTime,
					CreatedAt:           time.Now(),
				}

				newOrder.Status = newOrder.GetStatus(order.Status)
				newOrder.Type = newOrder.GetType(order.Type)
				newOrder.Side = newOrder.GetSide(order.Side)

				price, _ := strconv.ParseFloat(order.Price, 64)
				if price == 0.0 {
					price = cummulativeQuoteQuantity / executedQty
				}
				newOrder.Price = price

				_, err := dbConnect.Model(newOrder).
					Where("order_id = ?order_id").
					OnConflict("DO NOTHING").
					SelectOrInsert()

				if err != nil {
					fmt.Printf("add new order error: %v\n", err.Error())
					log.Warnf("add new order error: %v", err.Error())
				}
			}
		}

		getOrdersAccountsIsWorking = false
	}

	//Activate coins by balance
	_, err = dbConnect.Model((*Coin)(nil)).Exec(`
	UPDATE coins AS c SET is_enabled = 1, interval = '1m' WHERE id IN(
		SELECT b.coin_id
		FROM balances AS b
		INNER JOIN coins_pairs cp on b.coin_id = cp.coin_id AND cp.is_enabled = 1
		GROUP BY b.coin_id
    ) AND c.interval <> '1m';
`)

}

func getActualExchangeRate(message string) (string, error) {
	message = strings.ToUpper(strings.TrimSpace(message))

	var rate PercentCoin

	if !strings.Contains(message, "?") {
		return "", errors.New("no correct coin")
	}

	coin := strings.Replace(message, "?", "", 100)

	if len(coin) >= 10 {
		return "", errors.New("no correct coin")
	}

	res, err := dbConnect.Query(&rate, `

WITH coin_pairs_24_hours AS (
    SELECT k.coin_pair_id, c.id as coin_id, c.code, k.open, k.close, k.high, k.low, k.close_time, k.open_time, c.rank
    FROM klines AS k
             INNER JOIN coins_pairs AS cp ON cp.id = k.coin_pair_id
             INNER JOIN coins AS c ON c.id = cp.coin_id
    WHERE cp.couple = 'BUSD'
      AND c.is_enabled = 1
      AND cp.is_enabled = 1
      AND k.open_time >= NOW() - INTERVAL '1 DAY'
      AND c.code = ?
)

SELECT DISTINCT ON (t.coin_id) t.coin_id,
                               t.code,
                               t.rank,
                               minute10.percent AS minute10,
                               hour.percent     AS hour,
                               hour4.percent    AS hour4,
                               hour12.percent   AS hour12,
                               hour24.percent   AS hour24,
                               minute10.min_open   AS minute10_min_open,
                               minute10.max_close   AS minute10_max_close,
                               hour.min_open   AS hour_min_open,
                               hour.max_close   AS hour_max_close,
                               hour4.min_open   AS hour4_min_open,
                               hour4.max_close   AS hour4_max_close,
                               hour12.min_open   AS hour12_min_open,
                               hour12.max_close   AS hour12_max_close,
                               hour24.min_open   AS hour24_min_open,
                               hour24.max_close   AS hour24_max_close
FROM coin_pairs_24_hours AS t
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MIN(t.open) AS min_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MIN(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
	WHERE t.open_time >= date_round_down(NOW() - interval '10 MINUTE', '10 MINUTE')
    GROUP BY t.coin_pair_id
) as minute10 ON t.coin_pair_id = minute10.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MIN(t.open) AS min_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MIN(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
	WHERE t.open_time >= date_round_down(NOW() - interval '1 HOUR', '1 HOUR')
    GROUP BY t.coin_pair_id
) as hour ON t.coin_pair_id = hour.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MIN(t.open) AS min_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MIN(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
	WHERE t.open_time >= date_round_down(NOW() - interval '4 HOUR', '1 HOUR')
    GROUP BY t.coin_pair_id
) as hour4 ON t.coin_pair_id = hour4.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MIN(t.open) AS min_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MIN(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
	WHERE t.open_time >= date_round_down(NOW() - interval '12 HOUR', '1 HOUR')
    GROUP BY t.coin_pair_id
) as hour12 ON t.coin_pair_id = hour12.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MIN(t.open) AS min_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MIN(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
    GROUP BY t.coin_pair_id
) AS hour24 ON t.coin_pair_id = hour24.coin_pair_id
`, coin)

	if err != nil {
		log.Panic("can't get get actual exchange rate: %v", err)
		return "", err
	}

	if res.RowsAffected() == 0 {
		return "", errors.New("coin not found")
	}

	tableString := &strings.Builder{}
	table := tablewriter.NewWriter(tableString)
	table.SetHeader([]string{"Name", "Value"})

	table.Append([]string{"Coin id", IntToStr(int(rate.CoinId))})
	table.Append([]string{"Coin", rate.Code})
	table.Append([]string{"Rank", IntToStr(rate.Rank)})
	table.Append([]string{"10 Minute", FloatToStr(rate.Minute10)})
	table.Append([]string{"Hour", FloatToStr(rate.Hour)})
	table.Append([]string{"4 Hour", FloatToStr(rate.Hour4)})
	table.Append([]string{"12 Hour", FloatToStr(rate.Hour12)})
	table.Append([]string{"24 Hour", FloatToStr(rate.Hour24)})
	table.Append([]string{"10 Min open", FloatToStr(rate.Minute10MinOpen)})
	table.Append([]string{"10 Max close", FloatToStr(rate.Minute10MaxClose)})
	table.Append([]string{"Hour min open", FloatToStr(rate.HourMinOpen)})
	table.Append([]string{"Hour max close", FloatToStr(rate.HourMaxClose)})
	table.Append([]string{"4 Hour min open", FloatToStr(rate.Hour4MinOpen)})
	table.Append([]string{"4 Hour max close", FloatToStr(rate.Hour4MaxClose)})
	table.Append([]string{"12 Hour open", FloatToStr(rate.Hour12MinOpen)})
	table.Append([]string{"12 Hour max close", FloatToStr(rate.Hour12MaxClose)})
	table.Append([]string{"24 Hour min open", FloatToStr(rate.Hour24MinOpen)})
	table.Append([]string{"24 Hour max close", FloatToStr(rate.Hour24MaxClose)})

	table.Render()

	return tableString.String(), nil
}

func sendNotificationsAccounts() {

	fmt.Println("Notifications orders accounts work")

	var accounts []Account
	err := dbConnect.Model(&accounts).
		Where("account.is_enabled = ?", 1).
		Where("binance_api_key IS NOT NULL AND binance_secret_key IS NOT NULL").
		Select()

	if err != nil {
		log.Warnf("can't get accounts by get notificationsOrdersAccounts: %v", err)
		return
	}

	bot, err := tgbotapi.NewBotAPI(appConfig.TelegramBot)
	if err != nil {
		log.Warn(err)
		return
	}

	bot.Debug = false //!!!!

	for _, account := range accounts {

		notificationsAccountsText := getNotificationsAccountsText(account.Id)

		if notificationsAccountsText == "" {
			continue
		}
		msg := tgbotapi.NewMessage(account.TelegramId, "```"+notificationsAccountsText+"```")
		msg.ParseMode = "MarkdownV2"

		if _, err := bot.Send(msg); err != nil {
			if strings.Contains(err.Error(), "Forbidden: bot was blocked by the user") { // to const error text
				err := account.disableAccount()
				if err != nil {
					log.Warnf("Error disable subscriber: %v", err)
					continue
				}
			} else {
				log.Error(err)
			}
		}
	}
}

func getNotificationsAccountsText(accountId int64) string {

	var coins []PercentCoinShort
	err := getPercentCoinsByAccount(accountId, &coins)

	if err != nil {
		return "Возникла ошибка №435/1"
	}

	countCoins := len(coins)

	if countCoins == 0 {
		return ""
	}

	tableString := &strings.Builder{}
	table := tablewriter.NewWriter(tableString)
	table.SetHeader([]string{"Name", "10m", "1h", "4h", "12h", "24h"})
	table.SetCaption(true, "Balance.")
	for _, coin := range coins {
		table.Append([]string{
			coin.Code + "[" + IntToStr(coin.Rank) + "]",
			FloatToStr(coin.Minute10),
			FloatToStr(coin.Hour),
			FloatToStr(coin.Hour4),
			FloatToStr(coin.Hour12),
			FloatToStr(coin.Hour24),
		})
	}

	table.Render()

	return tableString.String()
}

func getPercentCoinsByAccount(accountId int64, coins *[]PercentCoinShort) (err error) {
	_, err = dbConnect.Query(coins, `

WITH coin_pairs_24_hours AS (
    SELECT k.coin_pair_id, c.id as coin_id, c.code, k.open,
           k.close, k.high, k.low, k.open_time, k.close_time, c.rank
    FROM klines AS k
             INNER JOIN coins_pairs AS cp ON cp.id = k.coin_pair_id
             INNER JOIN coins AS c ON c.id = cp.coin_id
    WHERE cp.couple = 'BUSD'
      AND c.is_enabled = 1
      AND cp.is_enabled = 1
      AND k.open_time >= NOW() - INTERVAL '1 DAY'
    AND c.id IN (
		SELECT t.coin_id FROM (
			SELECT DISTINCT ON (b.coin_id) b.coin_id, b.free, b.locked
			FROM balances AS b
			WHERE account_id = ?
			ORDER BY b.coin_id, b.created_at DESC
		) AS t
		WHERE t.free > 0 OR t.locked > 0
        )
    ORDER BY c.rank
)

SELECT *
FROM (
         SELECT t.*,
                ROUND(CAST((COALESCE(t.minute10, 0) + COALESCE(t.hour, 0) +
                            COALESCE(t.hour4, 0) + COALESCE(t.hour12, 0) +
                            COALESCE(t.hour24, 0)) AS NUMERIC), 3) AS percent_sum
         FROM (

                           SELECT DISTINCT ON (t.coin_id) t.coin_id,
                                                          t.code,
                                                          t.rank,
                                                          CAlC_PERCENT(MIN(COALESCE(minute10.first_open, 0)), MIN(COALESCE(minute10.last_close, 0))) AS minute10,
                                                          CAlC_PERCENT(MIN(COALESCE(hour.first_open, 0)), MIN(COALESCE(hour.last_close, 0)))         AS hour,
                                                          CAlC_PERCENT(MIN(COALESCE(hour4.first_open, 0)), MIN(COALESCE(hour4.last_close, 0)))       AS hour4,
                                                          CAlC_PERCENT(MIN(COALESCE(hour12.first_open, 0)), MIN(COALESCE(hour12.last_close, 0)))     AS hour12,
                                                          CAlC_PERCENT(MIN(COALESCE(hour24.first_open, 0)), MIN(COALESCE(hour24.last_close, 0)))     AS hour24

                           FROM coin_pairs_24_hours AS t
                                    LEFT JOIN (
                               SELECT t.coin_pair_id,
                                      MIN(t.open)                                       AS min_open,
                                      MAX(t.close)                                      AS max_close,
                                      (array_agg(t.open order by t.open_time asc))[1]   as first_open,
                                      (array_agg(t.close order by t.open_time desc))[1] as last_close
                               FROM coin_pairs_24_hours AS t
                               WHERE t.open_time >= date_round_down(NOW() - interval '10 MINUTE', '10 MINUTE')
                                  OR (t.open_time <= date_round_down(NOW() - interval '10 MINUTE', '10 MINUTE') AND
                                      t.close_time >= NOW())
                               GROUP BY t.coin_pair_id
                           ) as minute10 ON t.coin_pair_id = minute10.coin_pair_id
                                    LEFT JOIN (
                               SELECT t.coin_pair_id,
                                      MIN(t.open)                                       AS min_open,
                                      MAX(t.close)                                      AS max_close,
                                      (array_agg(t.open order by t.open_time asc))[1]   as first_open,
                                      (array_agg(t.close order by t.open_time desc))[1] as last_close
                               FROM coin_pairs_24_hours AS t
                               WHERE t.open_time >= date_round_down(NOW() - interval '1 HOUR', '1 HOUR')
                               GROUP BY t.coin_pair_id
                           ) as hour ON t.coin_pair_id = hour.coin_pair_id
                                    LEFT JOIN (
                               SELECT t.coin_pair_id,
                                      MIN(t.open)                                       AS min_open,
                                      MAX(t.close)                                      AS max_close,
                                      (array_agg(t.open order by t.open_time asc))[1]   as first_open,
                                      (array_agg(t.close order by t.open_time desc))[1] as last_close
                               FROM coin_pairs_24_hours AS t
                               WHERE t.open_time >= date_round_down(NOW() - interval '4 HOUR', '1 HOUR')
                               GROUP BY t.coin_pair_id
                           ) as hour4 ON t.coin_pair_id = hour4.coin_pair_id
                                    LEFT JOIN (
                               SELECT t.coin_pair_id,
                                      MIN(t.open)                                       AS min_open,
                                      MAX(t.close)                                      AS max_close,
                                      (array_agg(t.open order by t.open_time asc))[1]   as first_open,
                                      (array_agg(t.close order by t.open_time desc))[1] as last_close
                               FROM coin_pairs_24_hours AS t
                               WHERE t.open_time >= date_round_down(NOW() - interval '12 HOUR', '1 HOUR')
                               GROUP BY t.coin_pair_id
                           ) as hour12 ON t.coin_pair_id = hour12.coin_pair_id
                                    LEFT JOIN (
                               SELECT t.coin_pair_id,
                                      MIN(t.open)                                       AS min_open,
                                      MAX(t.close)                                      AS max_close,
                                      (array_agg(t.open order by t.open_time asc))[1]   as first_open,
                                      (array_agg(t.close order by t.open_time desc))[1] as last_close
                               FROM coin_pairs_24_hours AS t
                               GROUP BY t.coin_pair_id
                           ) AS hour24 ON t.coin_pair_id = hour24.coin_pair_id
                           GROUP BY t.coin_id, t.code, t.rank
                           ORDER BY t.coin_id
                           LIMIT 45
                       ) AS t
                        WHERE (
                                (t.minute10 >= 2 OR t.minute10 <= -2)
                                OR (t.hour >= 3 OR t.hour <= -3)
                                OR (t.hour4 >= 4 OR t.hour4 <= -4)
                                OR (t.hour12 >= 8 OR t.hour12 <= -8)
                                OR (t.hour24 >= 10 OR t.hour24 <= -10))
     ) AS t
ORDER BY percent_sum DESC;
`, accountId)

	if err != nil {
		log.Error("can't get percent pairs by accounts: %v", err)
		return err
	}

	return nil
}

func testSendImages() {

	fmt.Println("Notifications orders accounts work")

	var accounts []Account
	err := dbConnect.Model(&accounts).
		Where("account.is_enabled = ?", 1).
		Where("binance_api_key IS NOT NULL AND binance_secret_key IS NOT NULL").
		Select()

	if err != nil {
		log.Warnf("can't get accounts by get notificationsOrdersAccounts: %v", err)
		return
	}

	bot, err := tgbotapi.NewBotAPI(appConfig.TelegramBot)
	if err != nil {
		log.Warn(err)
		return
	}

	bot.Debug = false //!!!!

	chart.DefaultBackgroundColor = chart.ColorTransparent
	chart.DefaultCanvasColor = chart.ColorTransparent

	barWidth := 120

	var (
		colorWhite          = drawing.Color{R: 241, G: 241, B: 241, A: 255}
		colorMariner        = drawing.Color{R: 60, G: 100, B: 148, A: 255}
		colorLightSteelBlue = drawing.Color{R: 182, G: 195, B: 220, A: 255}
		colorPoloBlue       = drawing.Color{R: 126, G: 155, B: 200, A: 255}
		colorSteelBlue      = drawing.Color{R: 73, G: 120, B: 177, A: 255}
	)

	stackedBarChart := chart.StackedBarChart{
		Title:      "Quarterly Sales",
		TitleStyle: chart.StyleShow(),
		Background: chart.Style{
			Padding: chart.Box{
				Top: 100,
			},
		},
		//Width:      810,
		//Height:     500,
		XAxis:      chart.StyleShow(),
		YAxis:      chart.StyleShow(),
		BarSpacing: 10,
		Bars: []chart.StackedBar{
			{
				Name:  "Q1",
				Width: barWidth,
				Values: []chart.Value{
					{
						Label: "32K",
						Value: 32,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorMariner,
							FontColor:   colorWhite,
						},
					},
					{
						Label: "46K",
						Value: 46,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorLightSteelBlue,
							FontColor:   colorWhite,
						},
					},
					{
						Label: "48K",
						Value: 48,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorPoloBlue,
							FontColor:   colorWhite,
						},
					},
					{
						Label: "42K",
						Value: 42,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorSteelBlue,
							FontColor:   colorWhite,
						},
					},
				},
			},
			{
				Name:  "Q2",
				Width: barWidth,
				Values: []chart.Value{
					{
						Label: "45K",
						Value: 45,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorMariner,
							FontColor:   colorWhite,
						},
					},
					{
						Label: "60K",
						Value: 60,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorLightSteelBlue,
							FontColor:   colorWhite,
						},
					},
					{
						Label: "62K",
						Value: 62,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorPoloBlue,
							FontColor:   colorWhite,
						},
					},
					{
						Label: "53K",
						Value: 53,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorSteelBlue,
							FontColor:   colorWhite,
						},
					},
				},
			},
			{
				Name:  "Q3",
				Width: barWidth,
				Values: []chart.Value{
					{
						Label: "54K",
						Value: 54,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorMariner,
							FontColor:   colorWhite,
						},
					},
					{
						Label: "58K",
						Value: 58,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorLightSteelBlue,
							FontColor:   colorWhite,
						},
					},
					{
						Label: "55K",
						Value: 55,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorPoloBlue,
							FontColor:   colorWhite,
						},
					},
					{
						Label: "47K",
						Value: 47,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorSteelBlue,
							FontColor:   colorWhite,
						},
					},
				},
			},
			{
				Name:  "Q4",
				Width: barWidth,
				Values: []chart.Value{
					{
						Label: "46K",
						Value: 46,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorMariner,
							FontColor:   colorWhite,
						},
					},
					{
						Label: "70K",
						Value: 70,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorLightSteelBlue,
							FontColor:   colorWhite,
						},
					},
					{
						Label: "74K",
						Value: 74,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorPoloBlue,
							FontColor:   colorWhite,
						},
					},
					{
						Label: "60K",
						Value: 60,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorSteelBlue,
							FontColor:   colorWhite,
						},
					},
				},
			},
			{
				Name:  "Q1",
				Width: barWidth,
				Values: []chart.Value{
					{
						Label: "32K",
						Value: 32,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorMariner,
							FontColor:   colorWhite,
						},
					},
					{
						Label: "46K",
						Value: 46,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorLightSteelBlue,
							FontColor:   colorWhite,
						},
					},
					{
						Label: "48K",
						Value: 48,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorPoloBlue,
							FontColor:   colorWhite,
						},
					},
					{
						Label: "42K",
						Value: 42,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorSteelBlue,
							FontColor:   colorWhite,
						},
					},
				},
			},
			{
				Name:  "Q2",
				Width: barWidth,
				Values: []chart.Value{
					{
						Label: "45K",
						Value: 45,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorMariner,
							FontColor:   colorWhite,
						},
					},
					{
						Label: "60K",
						Value: 60,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorLightSteelBlue,
							FontColor:   colorWhite,
						},
					},
					{
						Label: "62K",
						Value: 62,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorPoloBlue,
							FontColor:   colorWhite,
						},
					},
					{
						Label: "53K",
						Value: 53,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorSteelBlue,
							FontColor:   colorWhite,
						},
					},
				},
			},
			{
				Name:  "Q3",
				Width: barWidth,
				Values: []chart.Value{
					{
						Label: "54K",
						Value: 54,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorMariner,
							FontColor:   colorWhite,
						},
					},
					{
						Label: "58K",
						Value: 58,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorLightSteelBlue,
							FontColor:   colorWhite,
						},
					},
					{
						Label: "55K",
						Value: 55,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorPoloBlue,
							FontColor:   colorWhite,
						},
					},
					{
						Label: "47K",
						Value: 47,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorSteelBlue,
							FontColor:   colorWhite,
						},
					},
				},
			},
			{
				Name:  "Q4",
				Width: barWidth,
				Values: []chart.Value{
					{
						Label: "46K",
						Value: 46,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorMariner,
							FontColor:   colorWhite,
						},
					},
					{
						Label: "70K",
						Value: 70,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorLightSteelBlue,
							FontColor:   colorWhite,
						},
					},
					{
						Label: "74K",
						Value: 74,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorPoloBlue,
							FontColor:   colorWhite,
						},
					},
					{
						Label: "60K",
						Value: 60,
						Style: chart.Style{
							StrokeWidth: .01,
							FillColor:   colorSteelBlue,
							FontColor:   colorWhite,
						},
					},
				},
			},
		},
	}

	buffer := bytes.NewBuffer([]byte{})
	err = stackedBarChart.Render(chart.PNG, buffer)

	for _, account := range accounts {

		photoFileBytes := tgbotapi.FileBytes{
			Name:  "picture",
			Bytes: buffer.Bytes(),
		}
		_, _ = bot.Send(tgbotapi.NewPhotoUpload(account.TelegramId, photoFileBytes))

		//notificationsAccountsText := getNotificationsAccountsText(account.Id)
		//
		//if notificationsAccountsText == "" {
		//	continue
		//}
		//
		//msg := tgbotapi.NewMessage(account.TelegramId, notificationsAccountsText)
		//if _, err := bot.Send(msg); err != nil {
		//	if strings.Contains(err.Error(), "Forbidden: bot was blocked by the user") { // to const error text
		//		err := account.disableAccount()
		//		if err != nil {
		//			log.Warnf("Error disable subscriber: %v", err)
		//			continue
		//		}
		//	} else {
		//		log.Error(err)
		//	}
		//}
	}
}
