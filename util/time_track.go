package util

import (
	"log"
	"time"
)

func TimeTrack(start time.Time, name string) {
	elapsed := time.Since(start).Seconds()
	log.Printf("%s took %0.4fs", name, elapsed)
}
