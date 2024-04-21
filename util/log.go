package util

import (
	"fmt"
	"reflect"
)

func LogMetric(metric interface{}) {
	// loop over the fields of the struct
	// and print the key and value
	fmt.Println("Metric:")
	fmt.Println("-------")
	v := reflect.ValueOf(metric)
	for i := 0; i < v.NumField(); i++ {
		fmt.Printf("%s: %v\n", v.Type().Field(i).Name, v.Field(i).Interface())
	}
	fmt.Println("-------")

}
