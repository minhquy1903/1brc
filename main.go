package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"time"
)

const (
	READ_BUFFER_SIZE = 1024 * 1024
	SEMICOLON        = 59
	END_LINE         = 10
)

type StationData struct {
	Name  string
	Min   float64
	Max   float64
	Sum   float64
	Count int
}

var result = make(map[string]*StationData)

func main() {
	defer TimeTrack(time.Now(), "execution time")

	file, err := os.Open("measurements_10m.txt")

	if err != nil {
		log.Fatalf("unable to read file: %v", err)
	}

	defer file.Close()
	buffer := make([]byte, READ_BUFFER_SIZE)
	leftoverBuffer := make([]byte, 1024)
	leftoverSize := 0

	for {
		n, err := file.Read(buffer)
		fmt.Println(n)
		l := 0

		for i := n - 1; i >= 0; i-- {
			// find the index of the last end line character
			if buffer[i] == END_LINE {
				l = i
				break
			}
		}

		data := make([]byte, l+leftoverSize)
		copy(data[:leftoverSize], leftoverBuffer)
		copy(data[leftoverSize:], buffer[:l])
		copy(leftoverBuffer, buffer[l+1:])
		leftoverSize = n - l - 1

		nextIdx := 0
		dataLen := len(data)

		for {
			if nextIdx > dataLen || dataLen == 0 {
				break
			}
			name, temperatureString, next := ProcessData(data[nextIdx:])
			nextIdx += next
			CalculateStationData(name, temperatureString)
		}

		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}
	}

	PrintResult(result)
}

func PrintResult(data map[string]*StationData) {
	result := make(map[string]*StationData, len(data))
	keys := make([]string, 0, len(data))
	for _, v := range data {
		keys = append(keys, v.Name)
		result[v.Name] = v
	}
	sort.Strings(keys)

	print("{")
	for _, k := range keys {
		v := result[k]
		fmt.Printf("%s=%.1f/%.1f/%.1f, ", k, v.Min, v.Sum/float64(v.Count), v.Max)
	}
	print("}\n")
}

func TimeTrack(start time.Time, name string) {
	elapsed := time.Since(start).Seconds()
	log.Printf("%s took %0.4fs", name, elapsed)
}

func ProcessData(data []byte) (string, string, int) {
	semicolon := 0
	n := len(data)
	endLine := n

	for i := 0; i < n; i++ {
		if data[i] == SEMICOLON {
			semicolon = i
		}

		if data[i] == END_LINE {
			endLine = i
			break
		}
	}

	return string(data[:semicolon]), string(data[semicolon+1 : endLine]), endLine + 1
}

func CalculateStationData(name string, temperatureString string) {
	temperature, err := strconv.ParseFloat(temperatureString, 64)
	if err != nil {
		fmt.Println(err)
		return
	}

	station, ok := result[name]
	if !ok {
		result[name] = &StationData{name, temperature, temperature, temperature, 1}
	} else {
		if temperature < station.Min {
			station.Min = temperature
		}
		if temperature > station.Max {
			station.Max = temperature
		}
		station.Sum += temperature
		station.Count++
	}

	if _, ok := result[name]; !ok {
		result[name].Max = temperature
	}
}
