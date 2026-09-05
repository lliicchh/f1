# 一份 Dockerfile 构建全部服务，靠 SVC 参数选择入口。
#
# 构建：docker build --build-arg SVC=lobby -t game/lobby:v1.0.0 .
FROM golang:1.25-alpine AS builder

ARG SVC
WORKDIR /src

RUN apk add --no-cache ca-certificates tzdata

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN test -n "$SVC" || (echo "必须指定 --build-arg SVC=<gateway|lobby|room|match|chat|world>" && false)
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${SVC}

FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -u 10001 game
ENV TZ=Asia/Shanghai

COPY --from=builder /out/app /usr/local/bin/app

USER game
# 7000 客户端连接（仅 gateway 用），9100 /metrics 与 /healthz
EXPOSE 7000 9100

ENTRYPOINT ["/usr/local/bin/app"]
