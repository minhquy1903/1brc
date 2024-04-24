v1:
	go run version_1/v1.go
	
v2:
	go run version_2/v2.go

v3:
	go run version_3/v3.go

v4:
	go run version_4/v4.go

goal:
	go run target/target.go

make statistics:
	go tool pprof -http 127.0.0.1:8080 cpu_profile.prof