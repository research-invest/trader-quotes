package models

import (
	"github.com/adshao/go-binance/v2"
	"time"
)

type Order struct {
	tableName struct{} `pg:"orders"`

	Id                  int64
	CoinPairId          int64 `pg:",coin_pair_id,foreign:orders_coin_pair_id_foreign"`
	AccountId           int64 `pg:",account_id,foreign:balances_account_id_foreign"`
	OrderId             int64 `pg:",order_id"`
	OrderListId         int64
	ClientOrderId       string
	Status              int8
	Type                int8
	Side                int8
	Price               float64   `pg:",price,use_zero"`
	OrigQty             float64   `pg:",orig_qty,use_zero"`
	ExecutedQty         float64   `pg:",executed_qty,use_zero"`
	CummulativeQuoteQty float64   `pg:",cummulative_quote_qty,use_zero"`
	StopPrice           float64   `pg:",stop_price,use_zero"`
	IcebergQty          float64   `pg:",iceberg_qty,use_zero"`
	OrigQuoteOrderQty   float64   `pg:",orig_quote_order_qty,use_zero"`
	Time                int64     `pg:",time"`
	UpdateTime          int64     `pg:",update_time"`
	CreatedAt           time.Time `pg:",created_at"`
	UpdatedAt           time.Time `pg:",updated_at"`
}

const (
	OrderStatusTypeNew             binance.OrderStatusType = "NEW"
	OrderStatusTypePartiallyFilled binance.OrderStatusType = "PARTIALLY_FILLED"
	OrderStatusTypeFilled          binance.OrderStatusType = "FILLED"
	OrderStatusTypeCanceled        binance.OrderStatusType = "CANCELED"
	OrderStatusTypePendingCancel   binance.OrderStatusType = "PENDING_CANCEL"
	OrderStatusTypeRejected        binance.OrderStatusType = "REJECTED"
	OrderStatusTypeExpired         binance.OrderStatusType = "EXPIRED"

	OrderTypeLimit              binance.OrderType = "LIMIT"
	OrderTypeMarket             binance.OrderType = "MARKET"
	OrderTypeStop               binance.OrderType = "STOP"
	OrderTypeStopMarket         binance.OrderType = "STOP_MARKET"
	OrderTypeTakeProfit         binance.OrderType = "TAKE_PROFIT"
	OrderTypeTakeProfitMarket   binance.OrderType = "TAKE_PROFIT_MARKET"
	OrderTypeTrailingStopMarket binance.OrderType = "TRAILING_STOP_MARKET"

	SideTypeBuy  binance.SideType = "BUY"
	SideTypeSell binance.SideType = "SELL"
)

func (o *Order) GetStatus(status binance.OrderStatusType) int8 {
	OrderStatuses := map[binance.OrderStatusType]int8{
		OrderStatusTypeNew:             1,
		OrderStatusTypePartiallyFilled: 2,
		OrderStatusTypeFilled:          3,
		OrderStatusTypeCanceled:        4,
		OrderStatusTypePendingCancel:   5,
		OrderStatusTypeRejected:        6,
		OrderStatusTypeExpired:         7,
	}

	if intStatus, ok := OrderStatuses[status]; ok {
		return intStatus
	}

	return 0
}

func (o *Order) GetType(orderType binance.OrderType) int8 {
	OrderTypes := map[binance.OrderType]int8{
		OrderTypeLimit:              1,
		OrderTypeMarket:             2,
		OrderTypeStop:               3,
		OrderTypeStopMarket:         4,
		OrderTypeTakeProfit:         5,
		OrderTypeTakeProfitMarket:   6,
		OrderTypeTrailingStopMarket: 7,
	}

	if intStatus, ok := OrderTypes[orderType]; ok {
		return intStatus
	}

	return 0
}

func (o *Order) GetSide(sideType binance.SideType) int8 {
	sideTypes := map[binance.SideType]int8{
		SideTypeBuy:  1,
		SideTypeSell: 2,
	}

	if intStatus, ok := sideTypes[sideType]; ok {
		return intStatus
	}

	return 0
}
