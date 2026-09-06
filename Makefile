SHELL := /bin/bash

SERVICES := gateway account lobby room match chat world
TAG      ?= dev
ENV_FILE ?= deploy/s1.env
PROJECT  ?= game-s1

GO      ?= go
GOFLAGS ?= -trimpath

.PHONY: all
all: tidy fmt vet test build

.PHONY: tidy
tidy:
	$(GO) mod tidy

.PHONY: fmt
fmt:
	gofmt -l -w $$(find . -name '*.go' -not -path './pkg/pb/*')

.PHONY: vet
vet:
	$(GO) vet ./...

.PHONY: test
test:
	$(GO) test -race -count=1 ./...

.PHONY: test-short
test-short:
	$(GO) test -count=1 ./...

.PHONY: cover
cover:
	$(GO) test -count=1 -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -20

.PHONY: build
build:
	@mkdir -p bin
	@for s in $(SERVICES); do \
		echo "building $$s"; \
		$(GO) build $(GOFLAGS) -o bin/$$s ./cmd/$$s || exit 1; \
	done
	@$(GO) build $(GOFLAGS) -o bin/gameconfctl ./cmd/gameconfctl

# ---- 游戏配置（评审 P1-3 / P1-4）----

# 导出内置默认配置。
.PHONY: conf-dump
conf-dump: build
	./bin/gameconfctl dump -o deploy/game.json

# 校验配置并打印版本号。发布前必跑。
.PHONY: conf-lint
conf-lint: build
	./bin/gameconfctl lint -f deploy/game.json

# 蒙特卡洛实测 RTP。数学模型改动后必跑，偏差超过 0.5% 会以非零码退出。
.PHONY: rtp
rtp: build
	./bin/gameconfctl rtp -f deploy/game.json -n 5000000

# 只跑 slots 的数学回归（比全量测试快得多，适合改数值时反复跑）。
.PHONY: test-rtp
test-rtp:
	$(GO) test -count=1 -run "TestRTP" -rtp.spins=5000000 ./internal/slots/ -v

# 生成 protobuf。需要 protoc 与 protoc-gen-go。
.PHONY: proto
proto:
	protoc --proto_path=proto --go_out=pkg/pb --go_opt=paths=source_relative proto/*.proto

.PHONY: images
images:
	@for s in $(SERVICES); do \
		echo "docker build game/$$s:$(TAG)"; \
		docker build --build-arg SVC=$$s -t game/$$s:$(TAG) . || exit 1; \
	done

# 起一整个区服。ENV_FILE 与 PROJECT 决定是哪一区（§11.1）。
.PHONY: up
up:
	ENV_FILE=$(ENV_FILE) TAG=$(TAG) docker compose -p $(PROJECT) up -d --build

.PHONY: down
down:
	ENV_FILE=$(ENV_FILE) TAG=$(TAG) docker compose -p $(PROJECT) down

.PHONY: logs
logs:
	ENV_FILE=$(ENV_FILE) TAG=$(TAG) docker compose -p $(PROJECT) logs -f --tail=200

.PHONY: ps
ps:
	ENV_FILE=$(ENV_FILE) TAG=$(TAG) docker compose -p $(PROJECT) ps

# 滚动重启单个服务，验证优雅下线与分片交接（§10.2 / §10.3）。
.PHONY: restart-lobby
restart-lobby:
	ENV_FILE=$(ENV_FILE) TAG=$(TAG) docker compose -p $(PROJECT) restart lobby-1

# 本地起依赖，方便不进容器直接跑服务。
.PHONY: deps-up
deps-up:
	ENV_FILE=$(ENV_FILE) TAG=$(TAG) docker compose -p $(PROJECT) up -d etcd nats redis

.PHONY: clean
clean:
	rm -rf bin coverage.out
