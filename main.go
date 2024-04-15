package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type Result struct {
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

	f, err := os.Open("measurements_10m.txt")

	if err != nil {
		log.Fatalf("unable to read file: %v", err)
	}

	defer f.Close()

	tmap := make(map[string]*Result)

	scanner := bufio.NewScanner(f) //scan the contents of a file and print line by line
	for scanner.Scan() {
		line := strings.Split(scanner.Text(), ";")

		temperature, err := strconv.ParseFloat(line[1], 64)
		if err != nil {
			log.Fatal(err)
		}

		if _, ok := tmap[line[0]]; !ok {
			tmap[line[0]] = &Result{
				Max:   temperature,
				Min:   temperature,
				Mean:  temperature,
				Count: 1,
				Total: 1,
			}
		} else {
			tmap[line[0]].Count += 1
			tmap[line[0]].Max = max(temperature, tmap[line[0]].Max)
			tmap[line[0]].Min = max(temperature, tmap[line[0]].Min)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading from file:", err) //print error if scanning is not done properly
	}

	fmt.Println(tmap)
}
