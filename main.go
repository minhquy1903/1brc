package main

import (
	"io"
	"log"
	"os"
	"time"
)

const (
	READ_BUFFER_SIZE = 1024 * 1024
	SEMICOLON        = 59
	END_LINE         = 10
)

type StationData struct {
	Max   float64
	Min   float64
	Mean  float64
	Total float64
	Count int32
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

func main() {
	defer timeTrack(time.Now(), "execution time")

	file, err := os.Open("measurements.txt")

	if err != nil {
		log.Fatalf("unable to read file: %v", err)
	}

	defer file.Close()
	buffer := make([]byte, READ_BUFFER_SIZE)
	leftoverBuffer := make([]byte, 1024)
	leftoverSize := 0

	for {
		n, err := file.Read(buffer)
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
			_, _, next := processData(data[nextIdx:])
			nextIdx += next
			// fmt.Println(string(name), string(temperature))
		}

		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}
	}
}

func processData(data []byte) ([]byte, []byte, int) {
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

	return data[:semicolon], data[semicolon+1 : endLine], endLine + 1
}
