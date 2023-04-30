VERSION=$(shell git describe --tags 2>/dev/null || echo ${CI_COMMIT_TAG:-"unknown"})
COMMIT=$(shell git rev-parse HEAD 2>/dev/null | head -c8)
MAKEDATE=$(shell date '+%FT%T%:z')
FILENAME=$(shell head -1 go.mod | awk -F '[/ ]' '{print $$NF}' | cut -d. -f1)
BIN_FILE=_dist/${FILENAME}
BUILD_CMD=go build -trimpath -ldflags "-s -w -X main.version=${VERSION} -X main.buildDate=${MAKEDATE} -X main.commit=${COMMIT}"
MAIN=./cmd
export CGO_ENABLED = 0

.PHONY: dist
dist:
	$(BUILD_CMD) -o $(BIN_FILE) $(MAIN)

DISTLIST=$(shell go tool dist list | grep -E '^(darwin|freebsd|linux|windows)/' | grep -Ev '/(386|mips)$$' | grep -v 'windows/arm' | sed 's~/~.~g')
PLATFORMS=$(foreach cmd,${DISTLIST},${cmd})
.PHONY: all
all: $(addprefix dist., $(PLATFORMS))

.PHONY: dist.%
dist.%:
	$(eval GOOS := $(word 1,$(subst ., ,$*)))
	$(eval GOARCH := $(word 2,$(subst ., ,$*)))
	@sh -c '[ "$(GOOS)" = "windows" ] && EXT=.exe; export GOOS=$(GOOS) GOARCH=$(GOARCH); set -x; $(BUILD_CMD) -o $(BIN_FILE)-$(GOOS)-$(GOARCH)$${EXT} $(MAIN)'

.PHONY: upx
upx:
	upx -9q --no-progress $(BIN_FILE)

.PHONY: upxall
upxall: $(addprefix upx., $(PLATFORMS))

.PHONY: upx.%
upx.%:
	$(eval GOOS := $(word 1,$(subst ., ,$*)))
	$(eval GOARCH := $(word 2,$(subst ., ,$*)))
	@sh -c '[ "$(GOOS)" = "windows" ] && EXT=.exe; export GOOS=$(GOOS) GOARCH=$(GOARCH); upx -9q --no-progress $(BIN_FILE)-$(GOOS)-$(GOARCH)$${EXT} || true'

deps:
	@go mod tidy -v && go mod verify && go mod download

clean:
	-rm -r _dist
