package main

import (
	"encoding/json"
	"os"
)

var appConfig Config

type Config struct {
	TelegramBot              string `json:"telegram-bot"`
	BinanceApiKeyReadOnly    string `json:"binance-api-key-read-only"`
	BinanceSecretKeyReadOnly string `json:"binance-secret-key-read-only"`
	Db                       Db
}

type Db struct {
	Host   string
	Port   int
	User   string
	Pass   string
	Dbname string
}

func readConfig() {
	file, err := os.Open("config.json")

	if err != nil {
		panic(err)
	}

	decoder := json.NewDecoder(file)
	appConfig = *(new(Config))
	err = decoder.Decode(&appConfig)
	if err != nil {
		panic(err)
	}
}
