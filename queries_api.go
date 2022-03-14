package main

import (
	"context"
	"github.com/go-redis/redis/v8"
)

const CounterQueriesApiPrefix = "counter_queries_api_"
const CounterQueriesApiErrorPrefix = "counter_queries_api_error_"

var ctxRedis = context.Background()
var rdb *redis.Client

func CounterQueriesApiSetZero() {
	_, err := rdb.Del(ctxRedis, CounterQueriesApiPrefix).Result()
	if err != nil {
		log.Panic("Error CounterQueriesApiSetZero : %v", err)
		panic(err)
	}
}

func CounterQueriesApiIncr() {
	_, err := rdb.Incr(ctxRedis, CounterQueriesApiPrefix).Result()
	if err != nil {
		log.Panic("Error CounterQueriesApiIncr : %v", err)
		panic(err)
	}
}

func CounterQueriesApiIncrError() {
	_, err := rdb.Incr(ctxRedis, CounterQueriesApiErrorPrefix).Result()

	if err != nil {
		log.Panic("Error CounterQueriesApiIncrError : %v", err)
		panic(err)
	}
}

func getCountQueriesApi() string {
	val, err := rdb.Get(ctxRedis, CounterQueriesApiPrefix).Result()
	if err == redis.Nil {
		return "0"
	} else if err != nil {
		log.Panic("Error getCountQueriesApi : %v", err)
		panic(err)
	}

	return val
}

func getCountQueriesApiError() string {
	val, err := rdb.Get(ctxRedis, CounterQueriesApiErrorPrefix).Result()

	if err == redis.Nil {
		return "0"
	} else if err != nil {
		log.Panic("Error getCountQueriesApiError : %v", err)
		panic(err)
	}

	return val
}

func redisInit() {
	rdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // too config
		Password: "",               // no password set
		DB:       0,                // use default DB
	})
}
