package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/minhquy1903/1brc/util"
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

type Result map[string]*StationData

var result = make(Result)

func main() {
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
	output := make(chan [2]string, workers)
	for i := 0; i < workers; i++ {
		go processBuffer(wg, input, output)
	}

	go func() {
		for v := range output {
			processStationData(v[0], v[1])
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

	printResult(result)
}

func readFile(f *os.File, wg *sync.WaitGroup) {
	readBuf := make([]byte, READ_BUFFER_SIZE)
	readBuffer := make([]byte, READ_BUFFER_SIZE)
	leftoverBuffer := make([]byte, 1024)
	leftoverSize := 0
	for {
		n, err := f.Read(readBuf)

		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatalf("unable to read file: %v", err)
		}

		l := 0
		for i := n - 1; i >= 0; i-- {
			// find the index of the last end line character
			if readBuf[i] == END_LINE {
				l = i
				break
			}
		}

		data := make([]byte, l+leftoverSize)
		copy(data[:leftoverSize], leftoverBuffer)
		copy(data[leftoverSize:], readBuffer[:l])
		copy(leftoverBuffer, readBuffer[l+1:])
		leftoverSize = n - l - 1

	}

}

func printResult(data map[string]*StationData) {
	result := make(map[string]*StationData, len(data))
	keys := make([]string, 0, len(data))
	for _, v := range data {
		keys = append(keys, v.Name)
		result[v.Name] = v
	}
	sort.Strings(keys)
	keyLength := len(keys)

	var pBuf bytes.Buffer

	pBuf.WriteString("{")
	for i := 0; i < keyLength-1; i++ {
		v := result[keys[i]]
		pBuf.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f, ", keys[i], v.Min, v.Sum/float64(v.Count), v.Max))
	}
	v := result[keys[keyLength-1]]
	pBuf.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f", keys[keyLength-1], v.Min, v.Sum/float64(v.Count), v.Max))
	pBuf.WriteString("}")

	fmt.Println(pBuf.String())

	if r := util.CheckResult(pBuf.Bytes(), "result_10m.txt"); !r {
		fmt.Println("Result is not correct")
	} else {
		fmt.Println("Result is correct")
	}
}

func processBuffer(wg *sync.WaitGroup, input <-chan []byte, output chan<- [2]string) {
	defer wg.Done()

	for data := range input {
		nextIdx := 0
		dataLen := len(data)

		for {
			if nextIdx > dataLen || dataLen == 0 {
				break
			}
			name, temperatureString, next := splitLine(data[nextIdx:])
			nextIdx += next
			output <- [2]string{name, temperatureString}
		}
	}
}

func splitLine(data []byte) (string, string, int) {
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

func processStationData(name string, temperatureString string) {
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
