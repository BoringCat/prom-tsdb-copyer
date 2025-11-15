ifdef CI_COMMIT_TAG
VERSION := ${CI_COMMIT_TAG}
else
# 从Git获取最新tag
VERSION := $(shell git describe --tags 2>/dev/null || echo "unknown")
endif

ifdef CI_COMMIT_SHORT_SHA
SHORT_COMMIT := ${CI_COMMIT_SHORT_SHA}
else
# 从Git获取当前提交ID，取前8位
SHORT_COMMIT := $(shell git rev-parse HEAD 2>/dev/null | head -c8)
endif

ifdef CI_COMMIT_SHA
COMMIT := ${CI_COMMIT_SHA}
else
# 从Git获取当前提交ID，取前8位
COMMIT := $(shell git rev-parse HEAD 2>/dev/null)
endif

ifdef CI_COMMIT_BRANCH
GIT_BRANCH := ${CI_COMMIT_BRANCH}
else
# 从Git获取当前提交ID，取前8位
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "HEAD")
endif

ifdef CI_PROJECT_NAME
FILENAME := ${CI_PROJECT_NAME}
else
# 从go.mod里面提取项目名，作为编译文件名
FILENAME := $(shell head -1 go.mod | awk -F '[/ ]' '{print $$NF}' | cut -d. -f1)
endif

# RFC3339Nano格式的编译时间
MAKEDATE  := $(shell date '+%FT%T%:z')
# Go版本号
GO_VERSION  := $(shell go version | cut -d' ' -f3)
# 定义输出目录（加上 "_" 避免 go 扫描）
DISTDIR   ?= _dist
# 定义最终输出路径
BIN_FILE  := ${DISTDIR}/${FILENAME}
# 编译命令
BUILD_CMD := go build -trimpath -ldflags "-s -w -X main.version=${VERSION} -X main.buildDate=${MAKEDATE} -X main.commit=${COMMIT} -X main.gitBranch=${GIT_BRANCH} -X main.goVersion=${GO_VERSION}"
# 入口文件或文件夹
MAIN      := ./
# 开启CGO
export CGO_ENABLED := 0
export NODE_VERSION := 20

# 默认命令：根据编译当前系统当前架构的版本，不添加 系统-架构 后缀
.PHONY: dist
dist:
	$(BUILD_CMD) -o $(BIN_FILE) $(MAIN)

# 获取支持的所有系统和架构，排除掉不想要的，作为编译目标
DISTLIST:=$(shell go tool dist list | grep -P '^(darwin|linux|windows)/' | grep -P '386|64|390' | sed 's~/~.~g')
# 从 系统.架构 列表中提取支持的系统（用于创建make入口）
OSLIST:=$(shell for t in ${DISTLIST}; do echo $${t}; done | cut -d. -f1 | sort | uniq)

# 定义make all操作为：make 所有系统
.PHONY: all
all: $(OSLIST)

# 定义 make 系统 操作为 dist.系统.所有架构
define OS_template
.PHONY: ${1}
${1}: $(addprefix dist., $(shell for t in ${DISTLIST}; do echo $${t}; done | grep ${1}))
endef
# 渲染所有 make dist.系统.架构
$(foreach os,$(OSLIST),$(eval $(call OS_template,$(os))))

# 定义 make dist.*，从输入中获取 GOOS=系统 和 GOARCH=架构。
# 且当 GOOS == windows 时，定义后缀为 .exe
.PHONY: dist.%
dist.%:
	$(eval GOOS := $(word 1,$(subst ., ,$*)))
	$(eval GOARCH := $(word 2,$(subst ., ,$*)))
	@bash -c '[ "$(GOOS)" = "windows" ] && EXT=.exe; export GOOS=$(GOOS) GOARCH=$(GOARCH); \
	[ "$(GOOS)" != "linux" ] || [ "$(GOARCH)" != "amd64" -a "$(GOARCH)" != "386" ] && export CGO_ENABLED=0; \
	set -x; \
	$(BUILD_CMD) -o $(BIN_FILE)-$(GOOS)-$(GOARCH)$${EXT} $(MAIN)'

# 定义 make dep.* 为执行 go mod 的操作
.PHONY: deps.%
dep.%:
	go mod $(word 1,$(subst ., ,$*))

# 定义 make deps 为执行依赖检查、校验、下载
deps: dep.tidy dep.verify dep.download

.PHONY: web.install
web.install:
	@bash -l -c 'cd web; nvm exec $(NODE_VERSION) yarn install'

.PHONY: web.build
web.build:
	@bash -l -c 'cd web; nvm exec $(NODE_VERSION) yarn build'

.PHONY: web
web: web.install web.build

# 定义 make clean 为清理目录和编译缓存
clean:
	-rm -rf -- ${DISTDIR}
	go tool dist clean
# 	如果你想清理掉所有缓存
# 	go clean -cache

