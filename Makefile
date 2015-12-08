test:
	go test -v ./...

install-qa:
	go get -u github.com/golang/lint/golint

qa:
	go fmt ./...
	go vet ./...
	golint ./...


refresh-deps:
	rm -rf Godeps
	godep save
