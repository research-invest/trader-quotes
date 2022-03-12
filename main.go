package main

//GOOS=linux GOARCH=amd64 go build -o ./trader -a

//https://api.binance.com/api/v3/klines?interval=1m&limit=20&symbol=AVAXBUSD

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/adshao/go-binance/v2"
	"github.com/cheggaaa/pb/v3"
	"github.com/go-pg/pg/v10"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
	"github.com/sirupsen/logrus"
	"os"
	"strconv"
	"strings"
	"time"
)

var dbConnect pg.DB
var log = logrus.New()
var getKlinesIsWorking, getAccountsInfoIsWorking bool

func main() {
	setLogParam()
	readConfig()

	dbInit()

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
			time.Sleep(30 * time.Minute)
		}
	}()

	//getAccountsInfo()

	for {
		s := time.Now().Second()

		if s == 0 {
			getKlines()
		}

		time.Sleep(1 * time.Second)
	}

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

	bot.Debug = false

	log.Printf("Authorized on account %s", bot.Self.UserName)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updates, _ := bot.GetUpdatesChan(u)

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

		if update.Message.IsCommand() { // ignore any non-command Messages

			//setapikey - Set binance api key read only
			//setsecretkey - Set binance secret key read only

			//addnewcoin - Add new coin - OLD
			//listallcoins - List all coins - OLD

			//getcountklines - get count klines - OLD
			//getcountcoins - get count coins - OLD
			//status - Status - OLD

			// Extract the command from the Message.
			switch update.Message.Command() {
			case "setapikey":
				apiKey := strings.Replace(update.Message.Text, "/setapikey", "", 1)
				err := account.saveApiKey(apiKey)
				if err != nil {
					msg.Text = "No correct api key"
					log.Warnf("can't save api key account: %v", err)

					break
				} else {
					msg.Text = "Api key saved."
					break
				}
			case "setsecretkey":
				secretKey := strings.Replace(update.Message.Text, "/setsecretkey", "", 1)
				err := account.saveSecretKey(secretKey)
				if err != nil {
					msg.Text = "No correct secret key"
					log.Warnf("can't save secret key account: %v", err)
					break
				} else {
					msg.Text = "Api secret key saved."
					break
				}
			case "getcountklines":
				//msg.Text = "Count klines: " + strconv.FormatInt(getCountKlines(), 10)
			case "getcountcoins":
				//msg.Text = "Count coins: " + strconv.FormatInt(getCountCoins(), 10)
			case "status":
				msg.Text = "I'm ok."
			default:
				msg.Text = "I don't know that command"
			}

			if _, err := bot.Send(msg); err != nil {
				log.Warnf("can't send bot message: %v", err)
			}

			continue
		}

		rate, err := getActualExchangeRate(update.Message.Text)

		if err == nil {
			s, _ := json.MarshalIndent(rate, "", "\t")
			msg.Text = string(s)
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

func getActualExchangeRate(message string) (PercentCoin, error) {
	message = strings.ToUpper(strings.TrimSpace(message))

	var rate PercentCoin

	if !strings.Contains(message, "?") {
		return rate, errors.New("no correct coin")
	}

	coin := strings.Replace(message, "?", "", 100)

	if len(coin) >= 10 {
		return rate, errors.New("no correct coin")
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
      AND k.close_time >= NOW() - INTERVAL '1 DAY'
      AND c.code = ?
    ORDER BY c.rank
)

SELECT DISTINCT ON (t.coin_id) t.coin_id,
	   t.code,
	   minute10.percent AS minute10,
	   hour.percent     AS hour,
	   hour4.percent    AS hour4,
	   hour12.percent   AS hour12,
	   hour24.percent   AS hour24,
	   minute10.max_open   AS minute10_max_open,
	   minute10.max_close   AS minute10_max_close,
	   hour.max_open   AS hour_max_open,
	   hour.max_close   AS hour_max_close,
	   hour4.max_open   AS hour4_max_open,
	   hour4.max_close   AS hour4_max_close,
	   hour12.max_open   AS hour12_max_open,
	   hour12.max_close   AS hour12_max_close,
	   hour24.max_open   AS hour24_max_open,
	   hour24.max_close   AS hour24_max_close
FROM coin_pairs_24_hours AS t
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MAX(t.open) AS max_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MAX(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
    WHERE t.open_time >= NOW() - INTERVAL '10 MINUTE' AND t.open_time <= NOW()
    GROUP BY t.coin_pair_id
) as minute10 ON t.coin_pair_id = minute10.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MAX(t.open) AS max_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MAX(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
    WHERE t.open_time >= NOW() - INTERVAL '1 HOUR' AND t.open_time <= NOW()
    GROUP BY t.coin_pair_id
) as hour ON t.coin_pair_id = hour.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MAX(t.open) AS max_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MAX(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
    WHERE t.open_time >= NOW() - INTERVAL '4 HOUR' AND t.open_time <= NOW()
    GROUP BY t.coin_pair_id
) as hour4 ON t.coin_pair_id = hour4.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MAX(t.open) AS max_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MAX(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
    WHERE t.open_time >= NOW() - INTERVAL '12 HOUR' AND t.open_time <= NOW()
    GROUP BY t.coin_pair_id
) as hour12 ON t.coin_pair_id = hour12.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MAX(t.open) AS max_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MAX(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
    WHERE t.open_time >= NOW() - INTERVAL '1 DAY' AND t.open_time <= NOW()
    GROUP BY t.coin_pair_id
) AS hour24 ON t.coin_pair_id = hour24.coin_pair_id
`, coin)

	if err != nil {
		log.Panic("can't get get actual exchange rate: %v", err)
		return rate, err
	}

	if res.RowsAffected() == 0 {
		return rate, errors.New("coin not found")
	}

	return rate, nil
}

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
		log.Panic(err)
		panic(err)
	}
}

func getPairs(pairs *[]Pair) (err error) {
	_, err = dbConnect.Query(pairs, `
	
WITH coin_pairs_close_time AS (
    SELECT DISTINCT ON (k.coin_pair_id) k.coin_pair_id, k.close_time
    FROM klines AS k
    INNER JOIN coins_pairs cp on cp.id = k.coin_pair_id
    INNER JOIN coins c on c.id = cp.coin_id
    WHERE cp.is_enabled = 1 AND c.is_enabled = 1
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
WHERE (t.close_time IS NULL OR t.close_time <= t.time_interval)
ORDER BY t.rank;
`)

	if err != nil {
		log.Panic("can't get pairs: %v", err)
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
		panic(err)
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
			Interval(pair.Interval).Limit(20).Do(context.Background())

		//if pair.CoinPairId == 3 {
		//	fmt.Println("klines")
		//	jsonF, _ := json.Marshal(klines)
		//	fmt.Println(string(jsonF))
		//}

		if err != nil {
			fmt.Println(err.Error())
			log.Warnf("get NewKlinesService error: %v", err)
			continue
		}

		for _, kline := range klines {
			open, _ := strconv.ParseFloat(kline.Open, 64)
			high, _ := strconv.ParseFloat(kline.High, 64)
			low, _ := strconv.ParseFloat(kline.Low, 64)
			closeKline, _ := strconv.ParseFloat(kline.Close, 64)
			quoteAssetVolume, _ := strconv.ParseFloat(kline.QuoteAssetVolume, 64)
			takerBuyBaseAssetVolume, _ := strconv.ParseFloat(kline.TakerBuyBaseAssetVolume, 64)
			takerBuyQuoteAssetVolume, _ := strconv.ParseFloat(kline.TakerBuyQuoteAssetVolume, 64)
			volume, _ := strconv.ParseFloat(kline.Volume, 64)

			newKline := &Kline{
				CoinPairId:               pair.CoinPairId,
				OpenTime:                 getTimestampFromMilliseconds(kline.OpenTime),
				CloseTime:                getTimestampFromMilliseconds(kline.CloseTime),
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

			_, err := dbConnect.Model(newKline).
				Where("coin_pair_id = ?coin_pair_id AND open_time = ?open_time AND close_time = ?close_time").
				OnConflict("DO NOTHING").
				SelectOrInsert()

			if err != nil {
				log.Warnf("add newKline error: %v", err.Error())
				fmt.Printf("add newKline error : %v\n", err.Error())
				if !strings.Contains(err.Error(), "ERROR #23505 duplicate key value violates unique constraint") {
					fmt.Printf("can't add a new file db record : %v\n", err.Error())
				}
			}
		}

		bar.Increment()
	}

	bar.Finish()

	getKlinesIsWorking = false

	//1499040000000,      // Open time
	//	"0.01634790",       // Open
	//	"0.80000000",       // High
	//	"0.01575800",       // Low
	//	"0.01577100",       // Close
	//	"148976.11427815",  // Volume
	//	1499644799999,      // Close time
	//	"2434.19055334",    // Quote asset volume
	//	308,                // Number of trades
	//	"1756.87402397",    // Taker buy base asset volume
	//	"28.46694368",      // Taker buy quote asset volume
	//	"17928899.62484339" // Ignore.

	//openOrders, err := client.NewListOpenOrdersService().Symbol("BNBETH").
	//	Do(context.Background())
	//if err != nil {
	//	fmt.Println(err)
	//	return
	//}
	//for _, o := range openOrders {
	//	fmt.Println(o)
	//}

}

func getAccountsInfo() {
	if getAccountsInfoIsWorking == true {
		return
	}

	getKlinesIsWorking = true

	fmt.Println("Get accounts info start work")

	var accounts []Account
	err := dbConnect.Model(&accounts).
		Where("is_enabled = ?", 1).
		Where("binance_api_key IS NOT NULL AND binance_secret_key IS NOT NULL").
		Select()

	if err != nil {
		log.Panic("can't get accounts: %v", err)
		panic(err)
	}

	for _, account := range accounts {
		client := binance.NewClient(account.BinanceApiKey, account.BinanceSecretKey)
		accountInfo, err := client.NewGetAccountService().Do(context.Background())

		if err != nil {
			//jsonF, _ := json.Marshal(err)
			//fmt.Println(string(jsonF))
			//{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}

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
				continue
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

			if err != nil {
				fmt.Printf("add new balance error: %v\n", err.Error())
				log.Warnf("add new balance error: %v", err.Error())
			}

		}

		getKlinesIsWorking = false
	}

	// create and start new bar

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
