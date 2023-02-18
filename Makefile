VERSION=$(shell git describe --tags 2>/dev/null || echo ${CI_COMMIT_TAG:-"unknown"})
COMMIT=$(shell git rev-parse HEAD 2>/dev/null | head -c8)
MAKEDATE=$(shell date '+%FT%T%:z')
FILENAME=$(shell head -1 go.mod | awk -F '[/ ]' '{print $$NF}' | cut -d. -f1)
BIN_FILE=_dist/${FILENAME}
BUILD_CMD=CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION} -X main.buildDate=${MAKEDATE} -X main.commit=${COMMIT}"
MAIN=.

.PHONY: deps dist
dist: deps
	$(BUILD_CMD) -o $(BIN_FILE) $(MAIN)

all: deps linux windows darwin
linux: deps linux-loong64 linux-amd64 linux-armv7 linux-arm64
windows: deps windows-386 windows-amd64 windows-arm
darwin: deps darwin-amd64 darwin-arm64

linux-loong64: deps
	@echo Building $(BIN_FILE)-linux-loong64...
	@GOOS=linux GOARCH=loong64 $(BUILD_CMD) -o $(BIN_FILE)-linux-loong64 $(MAIN)
	@echo Builded $(BIN_FILE)-linux-loong64
linux-amd64: deps
	@echo Building $(BIN_FILE)-linux-amd64...
	@GOOS=linux GOARCH=amd64 $(BUILD_CMD) -o $(BIN_FILE)-linux-amd64 $(MAIN)
	@upx -9 -q $(BIN_FILE)-linux-amd64
	@echo Builded $(BIN_FILE)-linux-amd64
linux-armv7: deps
	@echo Building $(BIN_FILE)-linux-armv7...
	@GOOS=linux GOARCH=arm $(BUILD_CMD) -o $(BIN_FILE)-linux-armv7 $(MAIN)
	@upx -9 -q $(BIN_FILE)-linux-armv7
	@echo Builded $(BIN_FILE)-linux-armv7
linux-arm64: deps
	@echo Building $(BIN_FILE)-linux-arm64...
	@GOOS=linux GOARCH=arm64 $(BUILD_CMD) -o $(BIN_FILE)-linux-arm64 $(MAIN)
	@upx -9 -q $(BIN_FILE)-linux-arm64
	@echo Builded $(BIN_FILE)-linux-arm64
windows-386: deps
	@echo Building $(BIN_FILE)-windows-386...
	@GOOS=windows GOARCH=386 $(BUILD_CMD) -o $(BIN_FILE)-windows-386.exe $(MAIN)
	@upx -9 -q $(BIN_FILE)-windows-386.exe
	@echo Builded $(BIN_FILE)-windows-386
windows-amd64: deps
	@echo Building $(BIN_FILE)-windows-amd64...
	@GOOS=windows GOARCH=amd64 $(BUILD_CMD) -o $(BIN_FILE)-windows-amd64.exe $(MAIN)
	@upx -9 -q $(BIN_FILE)-windows-amd64.exe
	@echo Builded $(BIN_FILE)-windows-amd64
windows-arm: deps
	@echo Building $(BIN_FILE)-windows-arm...
	@GOOS=windows GOARCH=arm $(BUILD_CMD) -o $(BIN_FILE)-windows-arm.exe $(MAIN)
	@upx -9 -q $(BIN_FILE)-windows-arm.exe
	@echo Builded $(BIN_FILE)-windows-arm
darwin-amd64: deps
	@echo Building $(BIN_FILE)-darwin-amd64...
	@GOOS=darwin GOARCH=amd64 $(BUILD_CMD) -o $(BIN_FILE)-darwin-amd64 $(MAIN)
	@upx -9 -q $(BIN_FILE)-darwin-amd64
	@echo Builded $(BIN_FILE)-darwin-amd64
darwin-arm64: deps
	@echo Building $(BIN_FILE)-darwin-arm64...
	@GOOS=darwin GOARCH=amd64 $(BUILD_CMD) -o $(BIN_FILE)-darwin-arm64 $(MAIN)
	@upx -9 -q $(BIN_FILE)-darwin-arm64
	@echo Builded $(BIN_FILE)-darwin-arm64

deps:
	@go mod tidy -v && go mod verify && go mod download

clean:
	-rm -r dist
