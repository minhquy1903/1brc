package model

type StationData struct {
	Name  string
	Min   int
	Max   int
	Sum   int
	Count int
}

type Result map[string]*StationData
