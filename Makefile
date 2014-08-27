service/client_rapid.go: service/service.go
	go run ./bin/tuplespaced/main.go --go > service/client_rapid.go~ && mv service/client_rapid.go~ service/client_rapid.go
