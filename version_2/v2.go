package main

import (
	"io"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/minhquy1903/1brc/model"
	"github.com/minhquy1903/1brc/util"
)

const (
	READ_BUFFER_SIZE = 1024 * 1024
	SEMICOLON        = 59
	END_LINE         = 10
)

var result = make(model.Result)

func main() {
	defer util.Statistic()()
	defer util.TimeTrack(time.Now(), "execution time")

	file, err := os.Open("measurements_10m.txt")

	if err != nil {
		log.Fatalf("unable to read file: %v", err)
	}

	defer file.Close()

	nCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nCPU)
	workers := nCPU * 2

	wg := new(sync.WaitGroup)
	wg.Add(workers)

	input := make(chan []byte, workers)
	output := make(chan model.Result, workers)

	for i := 0; i < workers; i++ {
		go processBuffer(wg, input, output)
	}

	go func() {
		for mapStation := range output {
			for k, v := range mapStation {
				station, ok := result[k]
				if !ok {
					result[k] = v
				} else {
					if v.Min < station.Min {
						station.Min = v.Min
					}
					if v.Max > station.Max {
						station.Max = v.Max
					}
					station.Sum += v.Sum
					station.Count += v.Count
				}
			}
		}
		close(output)
	}()

	readBuffer := make([]byte, READ_BUFFER_SIZE)
	leftoverBuffer := make([]byte, 1024)
	leftoverSize := 0

	for {
		n, err := file.Read(readBuffer)

		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		l := 0
		for i := n - 1; i >= 0; i-- {
			// find the index of the last end line character
			if readBuffer[i] == END_LINE {
				l = i
				break
			}
		}

		data := make([]byte, l+leftoverSize)
		copy(data[:leftoverSize], leftoverBuffer)
		copy(data[leftoverSize:], readBuffer[:l])
		copy(leftoverBuffer, readBuffer[l+1:])
		leftoverSize = n - l - 1

		input <- data
	}

	close(input)
	wg.Wait()

	util.PrintResult(result)
}

func processBuffer(wg *sync.WaitGroup, input <-chan []byte, output chan<- model.Result) {
	defer wg.Done()
	mapStation := make(model.Result)

	for data := range input {
		nextIdx := 0
		dataLen := len(data)

		for {
			if nextIdx > dataLen || dataLen == 0 {
				break
			}
			name, temperature, next := readLine(data[nextIdx:])
			nextIdx += next

			processLine(name, temperature, &mapStation)
		}
	}

	output <- mapStation
}

func readLine(data []byte) (string, int, int) {
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

	return string(data[:semicolon]), util.BytesToInt(data[semicolon+1 : endLine]), endLine + 1
}

func processLine(name string, temperature int, stationMap *model.Result) {
	station, ok := (*stationMap)[name]
	if !ok {
		(*stationMap)[name] = &model.StationData{Name: name, Min: temperature, Max: temperature, Sum: temperature, Count: 1}
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
}
