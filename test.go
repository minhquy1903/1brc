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
	consumer := make(chan int, 1000)
	wg := new(sync.WaitGroup)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(wg *sync.WaitGroup, input chan int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {

				mu.Lock()
				count++
				idx := count
				mu.Unlock()
				input <- idx
			}
		}(wg, consumer)
	}

	go func() {
		wg.Wait()
		close(consumer)
	}()

	for v := range consumer {
		fmt.Println(v)
	}

}
