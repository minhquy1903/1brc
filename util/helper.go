package util

import (
	"bytes"
	"fmt"
	"os"
	"runtime/pprof"
	"sort"
	"time"

	"github.com/minhquy1903/1brc/model"
)

const (
	DOT   = 46
	MINUS = 45
)

func BytesToInt(byteArray []byte) int {
	var result int
	negative := false

	for _, b := range byteArray {
		if b == DOT { // .
			continue
		}

		if b == MINUS { // -
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

func PrintResult(data model.Result) {
	result := make(model.Result, len(data))
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
		pBuf.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f, ", keys[i], float64(v.Min)/10, (float64(v.Sum)/10)/float64(v.Count), float64(v.Max)/10))
	}
	v := result[keys[keyLength-1]]
	pBuf.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f", keys[keyLength-1], float64(v.Min)/10, (float64(v.Sum)/10)/float64(v.Count), float64(v.Max)/10))
	pBuf.WriteString("}")

	fmt.Println(pBuf.String())

	if r := CheckResult(pBuf.Bytes(), "result_10m.txt"); !r {
		fmt.Println("\n#########################")
		fmt.Println("# Result is not correct #")
		fmt.Println("#########################")
	} else {
		fmt.Println("\n#####################")
		fmt.Println("# Result is correct #")
		fmt.Println("#####################")
	}
}

func CheckResult(fileBuf []byte, path string) bool {
	f, err := os.ReadFile(path)

	if err != nil {
		return false
	}

	if len(f) != len(fileBuf) {
		return false
	}

	for i := 0; i < len(f); i++ {
		if f[i] != fileBuf[i] {
			return false
		}
	}

	return true
}

func TimeTrack(start time.Time, name string) {
	elapsed := time.Since(start).Seconds()
	fmt.Printf("\n%s took %0.4fs\n", name, elapsed)
}

func Statistic() func() {
	f, err := os.Create("cpu_profile.prof")

	if err != nil {
		panic(err)
	}

	if err := pprof.StartCPUProfile(f); err != nil {
		panic(err)
	}
	return func() {
		pprof.StopCPUProfile()
		f.Close()
	}
}
