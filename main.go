package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

type Temperature struct {
	Location string
	Value    float32
}

type Result struct {
	Max   float32
	Min   float32
	Mean  float32
	Total float32
	Count int32
}

func main() {
	f, err := os.Open("measurements_10m.txt")

	if err != nil {
		log.Fatalf("unable to read file: %v", err)
	}

	defer f.Close()
	buf := make([]byte, 1024)

	tmap := make(map[string]Result)

	for {
		n, err := f.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println(err)
			continue
		}
		if n > 0 {
			r := strings.Split(string(buf[:n]), ";")
			fmt.Println(r)

			data := tmap[r[0]]

			if _, ok := tmap[r[0]]; !ok {
				tmap[r[0]] = Result{
					Max:   1,
					Min:   1,
					Mean:  1,
					Count: 1,
					Total: 1,
				}
			} else {
				tmap[r[0]].Count = 1
			}

		}
	}
}
