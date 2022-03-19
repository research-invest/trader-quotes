package main

import (
	"fmt"
)

func IntToStr(number int) string {
	return fmt.Sprintf("%v", number)
}

func FloatToStr(number float64) string {
	return fmt.Sprintf("%.2f", number)
}
