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
var mu sync.Mutex

// var readIdx int

func main() {
	defer util.Statistic()()

	defer util.TimeTrack(time.Now(), "execution time")

	nCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nCPU)
	workers := nCPU * 2

	wg := new(sync.WaitGroup)
	wg.Add(workers)

	output := make(chan model.Result, workers)
	leftOverChan := make(chan []byte)

	file, err := os.Open("measurements_10m.txt")

	if err != nil {
		log.Fatalf("unable to read file: %v", err)
	}

	defer file.Close()

	for i := 0; i < workers; i++ {
		go readFile(wg, file, output, leftOverChan)
	}

	go func() {
		wg.Wait()
		close(leftOverChan)
		close(output)
	}()

	// handle data output
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

	util.PrintResult(result)
}

func readFile(wg *sync.WaitGroup, f *os.File, output chan model.Result, leftOverChan chan []byte) {
	defer wg.Done()

	readBuf := make([]byte, READ_BUFFER_SIZE)
	data := make(model.Result)

	for {
		mu.Lock()
		// readIdx++
		// idx := readIdx
		n, err := f.Read(readBuf)
		mu.Unlock()

		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatalf("unable to read file: %v", err)
		}

		firstLineIdx := 0
		lastLineIdx := 0

		// get index of the first line
		for i := 0; i < n; i++ {
			if readBuf[i] == END_LINE {
				firstLineIdx = i
				break
			}
		}

		// fmt.Print(readBuf[:firstLineIdx+1])

		for i := n - 1; i >= 0; i-- {
			// find the index of the last end line character
			if readBuf[i] == END_LINE {
				lastLineIdx = i
				break
			}
		}

		// readBuf[lastLineIdx+1:]

		processBuffer(readBuf[firstLineIdx+1:lastLineIdx], &data, output)
	}

	output <- data
}

func processBuffer(data []byte, stationMap *model.Result, output chan model.Result) {
	nextIdx := 0
	dataLen := len(data)

	for {
		if nextIdx > dataLen || dataLen == 0 {
			break
		}
		name, temperature, next := readLine(data[nextIdx:])
		nextIdx += next
		processLine(name, temperature, stationMap)
	}
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

	return string(data[:semicolon]), bytesToInt(data[semicolon+1 : endLine]), endLine + 1
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

func bytesToInt(byteArray []byte) int {
	var result int
	negative := false

	for _, b := range byteArray {
		if b == 46 { // .
			continue
		}

		if b == 45 { // -
			negative = true
			continue
		}
		result = result*10 + int(b-48)
	}

	if negative {
		return -result
	}

	return result
}
