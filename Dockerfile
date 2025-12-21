# Build stage
FROM golang:1.24 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o mqueue ./main.go

# Final stage
FROM alpine:latest
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder /app/mqueue .
COPY .env ./
COPY mqueue-certs/cert.pem ./mqueue-certs/cert.pem
COPY mqueue-certs/key.pem ./mqueue-certs/key.pem
RUN mkdir -p /app/wal/shard0 /app/wal/shard1
RUN touch /app/wal/shard0/wal.log /app/wal/shard1/wal.log
EXPOSE 8080 2112
CMD ["./mqueue"]