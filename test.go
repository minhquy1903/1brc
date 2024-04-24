package main

import (
	"fmt"
	"runtime"
	"sync"
)

func main() {
	workers := runtime.NumCPU()
	count := 0
	var mu sync.Mutex
	consumer := make(chan int, workers)
	wg := new(sync.WaitGroup)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(wg *sync.WaitGroup, input chan int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {

				mu.Lock()
				idx := count + 1
				input <- idx
				mu.Unlock()
			}
		}(wg, consumer)
	}

	go func() {
		for v := range consumer {
			fmt.Println(v)
		}
	}()

	wg.Wait()
	close(consumer)

}
