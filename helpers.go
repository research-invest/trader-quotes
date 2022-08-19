package main

import (
	"fmt"
	"math/rand"
)

func IntToStr(number int) string {
	return fmt.Sprintf("%v", number)
}

func FloatToStr(number float64) string {
	return fmt.Sprintf("%.2f", number)
}

func findMinAndMax(a []float64) (min float64, max float64) {
	min = a[0]
	max = a[0]
	for _, value := range a {
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}
	return min, max
}

func random(min, max float64) float64 {
	return rand.Float64()*(max-min) + min
}
