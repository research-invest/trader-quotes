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
	"math/rand"
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

	//go func() {
	//	for {
	//		getAccountsInfo()
	//		getOrdersAccounts()
	//		sendNotificationsAccounts()
	//		time.Sleep(30 * time.Minute)
	//	}
	//}()

	testSendImages()

	//getAccountsInfo()

	//for {
	//	t := time.Now()
	//
	//	if t.Hour() >= 3 && t.Hour() < 6 {
	//		time.Sleep(1 * time.Hour) // temp
	//	}
	//
	//	if t.Hour() == 0 && t.Minute() == 0 {
	//		CounterQueriesApiSetZero()
	//	}
	//
	//	if t.Second() == 0 {
	//		getKlines()
	//		time.Sleep(1 * time.Minute)
	//	}
	//}
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
			//default:
			//	msg.Text = "I don't know that command"
		}

		if _, err := bot.Send(msg); err != nil {
			log.Warnf("can't send bot message: %v", err)
		}

		//continue
		//}

		if update.Message.Text == "" {
			continue
		}

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

	xv, yv := xvalues(), yvalues()

	priceSeries := chart.TimeSeries{
		Name: "SPY",
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.GetDefaultColor(0),
		},
		XValues: xv,
		YValues: yv,
	}

	smaSeries := chart.SMASeries{ // красная линия
		Name: "SPY - SMA",
		Style: chart.Style{
			Show:            true,
			StrokeColor:     drawing.ColorRed,
			StrokeDashArray: []float64{5.0, 5.0},
		},
		InnerSeries: priceSeries,
	}

	bbSeries := &chart.BollingerBandsSeries{ //фоновый
		Name: "SPY - Bol. Bands",
		Style: chart.Style{
			Show:        true,
			StrokeColor: drawing.ColorFromHex("efefef"),
			FillColor:   drawing.ColorFromHex("efefef").WithAlpha(64),
		},
		InnerSeries: priceSeries,
	}

	graph := chart.Chart{
		XAxis: chart.XAxis{
			Style:        chart.Style{Show: true},
			TickPosition: chart.TickPositionBetweenTicks,
		},
		YAxis: chart.YAxis{
			Style: chart.Style{Show: true},
			Range: &chart.ContinuousRange{
				Max: 220.0,
				Min: 180.0,
			},
		},
		Series: []chart.Series{
			bbSeries,
			priceSeries,
			smaSeries,
		},
	}

	//----

	buffer := bytes.NewBuffer([]byte{})
	err = graph.Render(chart.PNG, buffer)

	for _, account := range accounts {

		photoFileBytes := tgbotapi.FileBytes{
			Name:  "picture",
			Bytes: buffer.Bytes(),
		}

		photo := tgbotapi.NewPhoto(account.TelegramId, photoFileBytes)

		if _, err := bot.Send(photo); err != nil {
			fmt.Println(err.Error())
		}

		//mp := tgbotapi.NewInputMediaPhoto(photoFileBytes)
		//_, _ = bot.Send(account.TelegramId, mp)

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

func random(min, max float64) float64 {
	return rand.Float64()*(max-min) + min
}

func xvalues() []time.Time {
	rawx := []string{"2015-07-17", "2015-07-20", "2015-07-21", "2015-07-22", "2015-07-23", "2015-07-24", "2015-07-27",
		"2015-07-28", "2015-07-29", "2015-07-30", "2015-07-31", "2015-08-03", "2015-08-04", "2015-08-05", "2015-08-06",
		"2015-08-07", "2015-08-10", "2015-08-11", "2015-08-12", "2015-08-13", "2015-08-14", "2015-08-17", "2015-08-18",
		"2015-08-19", "2015-08-20", "2015-08-21", "2015-08-24", "2015-08-25", "2015-08-26", "2015-08-27", "2015-08-28",
		"2015-08-31", "2015-09-01", "2015-09-02", "2015-09-03", "2015-09-04", "2015-09-08", "2015-09-09", "2015-09-10",
		"2015-09-11", "2015-09-14", "2015-09-15", "2015-09-16", "2015-09-17", "2015-09-18", "2015-09-21", "2015-09-22",
		"2015-09-23", "2015-09-24", "2015-09-25", "2015-09-28", "2015-09-29", "2015-09-30", "2015-10-01", "2015-10-02",
		"2015-10-05", "2015-10-06", "2015-10-07", "2015-10-08", "2015-10-09", "2015-10-12", "2015-10-13", "2015-10-14",
		"2015-10-15", "2015-10-16", "2015-10-19", "2015-10-20", "2015-10-21", "2015-10-22", "2015-10-23", "2015-10-26",
		"2015-10-27", "2015-10-28", "2015-10-29", "2015-10-30", "2015-11-02", "2015-11-03", "2015-11-04", "2015-11-05",
		"2015-11-06", "2015-11-09", "2015-11-10", "2015-11-11", "2015-11-12", "2015-11-13", "2015-11-16", "2015-11-17",
		"2015-11-18", "2015-11-19", "2015-11-20", "2015-11-23", "2015-11-24", "2015-11-25", "2015-11-27", "2015-11-30",
		"2015-12-01", "2015-12-02", "2015-12-03", "2015-12-04", "2015-12-07", "2015-12-08", "2015-12-09", "2015-12-10",
		"2015-12-11", "2015-12-14", "2015-12-15", "2015-12-16", "2015-12-17", "2015-12-18", "2015-12-21", "2015-12-22",
		"2015-12-23", "2015-12-24", "2015-12-28", "2015-12-29", "2015-12-30", "2015-12-31", "2016-01-04", "2016-01-05",
		"2016-01-06", "2016-01-07", "2016-01-08", "2016-01-11", "2016-01-12", "2016-01-13", "2016-01-14", "2016-01-15",
		"2016-01-19", "2016-01-20", "2016-01-21", "2016-01-22", "2016-01-25", "2016-01-26", "2016-01-27", "2016-01-28",
		"2016-01-29", "2016-02-01", "2016-02-02", "2016-02-03", "2016-02-04", "2016-02-05", "2016-02-08", "2016-02-09",
		"2016-02-10", "2016-02-11", "2016-02-12", "2016-02-16", "2016-02-17", "2016-02-18", "2016-02-19", "2016-02-22",
		"2016-02-23", "2016-02-24", "2016-02-25", "2016-02-26", "2016-02-29", "2016-03-01", "2016-03-02", "2016-03-03",
		"2016-03-04", "2016-03-07", "2016-03-08", "2016-03-09", "2016-03-10", "2016-03-11", "2016-03-14", "2016-03-15",
		"2016-03-16", "2016-03-17", "2016-03-18", "2016-03-21", "2016-03-22", "2016-03-23", "2016-03-24", "2016-03-28",
		"2016-03-29", "2016-03-30", "2016-03-31", "2016-04-01", "2016-04-04", "2016-04-05", "2016-04-06", "2016-04-07",
		"2016-04-08", "2016-04-11", "2016-04-12", "2016-04-13", "2016-04-14", "2016-04-15", "2016-04-18", "2016-04-19",
		"2016-04-20", "2016-04-21", "2016-04-22", "2016-04-25", "2016-04-26", "2016-04-27", "2016-04-28", "2016-04-29",
		"2016-05-02", "2016-05-03", "2016-05-04", "2016-05-05", "2016-05-06", "2016-05-09", "2016-05-10", "2016-05-11",
		"2016-05-12", "2016-05-13", "2016-05-16", "2016-05-17", "2016-05-18", "2016-05-19", "2016-05-20", "2016-05-23",
		"2016-05-24", "2016-05-25", "2016-05-26", "2016-05-27", "2016-05-31", "2016-06-01", "2016-06-02", "2016-06-03",
		"2016-06-06", "2016-06-07", "2016-06-08", "2016-06-09", "2016-06-10", "2016-06-13", "2016-06-14", "2016-06-15",
		"2016-06-16", "2016-06-17", "2016-06-20", "2016-06-21", "2016-06-22", "2016-06-23", "2016-06-24", "2016-06-27",
		"2016-06-28", "2016-06-29", "2016-06-30", "2016-07-01", "2016-07-05", "2016-07-06", "2016-07-07", "2016-07-08",
		"2016-07-11", "2016-07-12", "2016-07-13", "2016-07-14", "2016-07-15"}

	var dates []time.Time
	for _, ts := range rawx {
		parsed, _ := time.Parse(chart.DefaultDateFormat, ts)
		dates = append(dates, parsed)
	}
	return dates
}

func yvalues() []float64 {
	return []float64{212.47, 212.59, 211.76, 211.37, 210.18, 208.00, 206.79, 209.33, 210.77, 210.82, 210.50,
		209.79, 209.38, 210.07, 208.35, 207.95, 210.57, 208.66, 208.92, 208.66, 209.42, 210.59, 209.98, 208.32,
		203.97, 197.83, 189.50, 187.27, 194.46, 199.27, 199.28, 197.67, 191.77, 195.41, 195.55, 192.59, 197.43,
		194.79, 195.85, 196.74, 196.01, 198.45, 200.18, 199.73, 195.45, 196.46, 193.90, 193.60, 192.90, 192.87,
		188.01, 188.12, 191.63, 192.13, 195.00, 198.47, 197.79, 199.41, 201.21, 201.33, 201.52, 200.25, 199.29,
		202.35, 203.27, 203.37, 203.11, 201.85, 205.26, 207.51, 207.00, 206.60, 208.95, 208.83, 207.93, 210.39,
		211.00, 210.36, 210.15, 210.04, 208.08, 208.56, 207.74, 204.84, 202.54, 205.62, 205.47, 208.73, 208.55,
		209.31, 209.07, 209.35, 209.32, 209.56, 208.69, 210.68, 208.53, 205.61, 209.62, 208.35, 206.95, 205.34,
		205.87, 201.88, 202.90, 205.03, 208.03, 204.86, 200.02, 201.67, 203.50, 206.02, 205.68, 205.21, 207.40,
		205.93, 203.87, 201.02, 201.36, 198.82, 194.05, 191.92, 192.11, 193.66, 188.83, 191.93, 187.81, 188.06,
		185.65, 186.69, 190.52, 187.64, 190.20, 188.13, 189.11, 193.72, 193.65, 190.16, 191.30, 191.60, 187.95,
		185.42, 185.43, 185.27, 182.86, 186.63, 189.78, 192.88, 192.09, 192.00, 194.78, 192.32, 193.20, 195.54,
		195.09, 193.56, 198.11, 199.00, 199.78, 200.43, 200.59, 198.40, 199.38, 199.54, 202.76, 202.50, 202.17,
		203.34, 204.63, 204.38, 204.67, 204.56, 203.21, 203.12, 203.24, 205.12, 206.02, 205.52, 206.92, 206.25,
		204.19, 206.42, 203.95, 204.50, 204.02, 205.92, 208.00, 208.01, 207.78, 209.24, 209.90, 210.10, 208.97,
		208.97, 208.61, 208.92, 209.35, 207.45, 206.33, 207.97, 206.16, 205.01, 204.97, 205.72, 205.89, 208.45,
		206.50, 206.56, 204.76, 206.78, 204.85, 204.91, 204.20, 205.49, 205.21, 207.87, 209.28, 209.34, 210.24,
		209.84, 210.27, 210.91, 210.28, 211.35, 211.68, 212.37, 212.08, 210.07, 208.45, 208.04, 207.75, 208.37,
		206.52, 207.85, 208.44, 208.10, 210.81, 203.24, 199.60, 203.20, 206.66, 209.48, 209.92, 208.41, 209.66,
		209.53, 212.65, 213.40, 214.95, 214.92, 216.12, 215.83}
}
