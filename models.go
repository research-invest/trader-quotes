package main

import (
	"errors"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
	"strings"
	"time"
)

const (
	IS_ENABLED_TRUE  = 1
	IS_ENABLED_FALSE = 0
)

type Account struct {
	tableName struct{} `pg:"accounts"`

	Id                int64
	IsEnabled         int8   `pg:",is_enabled,use_zero"`
	TelegramId        int64  `pg:",telegram_id"`
	MakerCommission   int32  `pg:",maker_commission"`
	TakerCommission   int32  `pg:",taker_commission"`
	BuyerCommission   int32  `pg:",buyer_commission"`
	SellerCommission  int32  `pg:",seller_commission"`
	TelegramFirstName string `pg:",telegram_first_name"`
	TelegramLastName  string `pg:",telegram_last_name"`
	TelegramUsername  string `pg:",telegram_username"`
	BinanceApiKey     string `pg:",binance_api_key"`
	BinanceSecretKey  string `pg:",binance_secret_key"`
	Email             string
	CreatedAt         time.Time `pg:",created_at"`
	UpdatedAt         time.Time `pg:",updated_at"`
	//Balances          *[]Balance `pg:",fk:file_id"`
	//Balances []Balance `pg:"polymorphic,join_fk:account_id"`
}

func (a *Account) addNew(data *tgbotapi.Chat) (acc *Account, err error) {
	newAccount := &Account{
		IsEnabled:         IS_ENABLED_TRUE,
		TelegramId:        data.ID,
		TelegramFirstName: data.FirstName,
		TelegramLastName:  data.LastName,
		TelegramUsername:  data.UserName,
		CreatedAt:         time.Now(),
	}

	_, err = dbConnect.Model(newAccount).
		Where("telegram_id = ?telegram_id").
		OnConflict("(telegram_id) DO UPDATE").
		Set("is_enabled = ?is_enabled").
		Insert()

	return newAccount, err
}

func (a *Account) saveApiKey(apiKey string) (err error) {
	apiKey = strings.TrimSpace(apiKey)

	if len(apiKey) != 64 {
		return errors.New("api-key not correct")
	}

	a.BinanceApiKey = apiKey
	a.UpdatedAt = time.Now()
	_, err = dbConnect.Model(a).
		Set("binance_api_key = ?binance_api_key").
		Set("updated_at = ?updated_at").
		Where("id = ?id").
		Update()

	return err
}

func (a *Account) saveSecretKey(secretKey string) (err error) {
	secretKey = strings.TrimSpace(secretKey)

	if len(secretKey) != 64 {
		return errors.New("secret key not correct")
	}

	a.BinanceSecretKey = secretKey
	a.UpdatedAt = time.Now()
	_, err = dbConnect.Model(a).
		Set("binance_secret_key = ?binance_secret_key").
		Set("updated_at = ?updated_at").
		Where("id = ?id").
		Update()

	return err
}

func (a *Account) getCoinsInBalance() []BalanceCoin {
	var coins []BalanceCoin

	_, err := dbConnect.Query(&coins, `
SELECT c.id, c.code, c.code || cp.couple AS pair, cp.id AS pair_id
FROM coins AS c
INNER JOIN coins_pairs cp on c.id = cp.coin_id AND cp.is_enabled = 1
WHERE c.id IN(
    SELECT t.coin_id FROM (
		SELECT DISTINCT ON (b.coin_id) b.coin_id, b.free, b.locked
		FROM balances AS b
		WHERE account_id = ?
		ORDER BY b.coin_id, b.created_at DESC
    ) AS t
    WHERE t.free > 0 OR t.locked > 0
)
AND c.is_enabled = 1;
`, a.Id)

	if err != nil {
		log.Panic("can't get coins in balance: %v", err)
		return nil
	}

	return coins
}

func (a *Account) disableAccount() (err error) {
	a.IsEnabled = IS_ENABLED_FALSE
	//a.BinanceApiKey = ""
	//a.BinanceSecretKey = "" todo раскоментить
	a.UpdatedAt = time.Now()
	_, err = dbConnect.Model(a).
		Set("is_enabled = ?is_enabled").
		Set("updated_at = ?updated_at").
		Where("id = ?id").
		Update()

	return err
}

type Kline struct {
	tableName struct{} `pg:"klines"`

	Id                       int64
	CoinPairId               int64     //`pg:",coin_pair_id,foreign:klines_coin_pair_id_fkey"`
	OpenTime                 time.Time //`pg:",open_time"`
	CloseTime                time.Time //`pg:",close_time"`
	Open                     float64   //`pg:",open"`
	High                     float64   //`pg:",high"`
	Low                      float64   //`pg:",low"`
	Close                    float64   //`pg:",close"`
	Volume                   float64   `pg:",volume,use_zero"`
	QuoteAssetVolume         float64   `pg:",quote_asset_volume,use_zero"`
	TradeNum                 int64     `pg:",trade_num,use_zero"`
	TakerBuyBaseAssetVolume  float64   `pg:",taker_buy_base_asset_volume,use_zero"`
	TakerBuyQuoteAssetVolume float64   `pg:",taker_buy_quote_asset_volume,use_zero"`
	RatioOpenClose           float64   //`pg:",ratio_open_close"`
	RatioHighLow             float64   //`pg:",ratio_high_low"`
}

type Pair struct {
	Pair       string
	CoinName   string
	CoinId     int64
	CoinPairId int64
	Cap        float64
	Interval   string
}

type Balance struct {
	tableName struct{} `pg:"balances"`

	Id        int64
	CoinId    int64     `pg:",coin_id,foreign:balances_coin_id_foreign"`
	AccountId int64     `pg:",account_id,foreign:balances_account_id_foreign"`
	Free      float64   `pg:",free,use_zero"`
	Locked    float64   `pg:",locked,use_zero"`
	CreatedAt time.Time `pg:",created_at"`
	UpdatedAt time.Time `pg:",updated_at"`
	Coin      *Coin     `pg:",fk:coin_id"`
}

type Coin struct {
	tableName struct{} `pg:"coins"`

	Id        int64
	Name      string
	Code      string
	Icon      string
	Interval  string
	IsEnabled int8      `pg:",is_enabled,use_zero"`
	CreatedAt time.Time `pg:",created_at"`
	UpdatedAt time.Time `pg:",updated_at"`

	Rank int32
}

type BalanceCoin struct {
	Id     int64
	Pair   string `pg:",pair,use_zero"`
	Code   string
	PairId int64
}

type PercentCoin struct {
	CoinId           int64
	Rank             int
	Code             string
	Minute10         float64
	Hour             float64
	Hour4            float64
	Hour12           float64
	Hour24           float64
	Minute10MinOpen  float64
	Minute10MaxClose float64
	HourMinOpen      float64
	HourMaxClose     float64
	Hour4MinOpen     float64
	Hour4MaxClose    float64
	Hour12MinOpen    float64
	Hour12MaxClose   float64
	Hour24MinOpen    float64
	Hour24MaxClose   float64
}

type PercentCoinShort struct {
	CoinId     int64
	Rank       int
	Code       string
	Minute10   float64
	Hour       float64
	Hour4      float64
	Hour12     float64
	Hour24     float64
	PercentSum float64
}

type BalanceInfo struct {
	Code     string
	Rank     int
	Quantity float64
	Price    float64
	Sum      float64
}

type AvgPrice struct {
	Id       int64
	Code     string
	Price    float64
	AvgPrice float64
}
