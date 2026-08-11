FROM golang:1.23-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -o /out/queuemaxxing ./cmd/server
FROM alpine:3.20
WORKDIR /app
COPY --from=build /out/queuemaxxing /app/queuemaxxing
COPY web /app/web
VOLUME ["/data"]
EXPOSE 8080
ENTRYPOINT ["/app/queuemaxxing","-data","/data/queue.wal"]
