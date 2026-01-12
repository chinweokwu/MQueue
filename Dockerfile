# Build Stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git make

# Copy module files first (caching)
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build distinct binaries
# CGO_ENABLED=0 for static binaries
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o mqueue ./main.go

# Production Stage
FROM gcr.io/distroless/static-debian12 AS production

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/mqueue .

# Expose HTTP port
EXPOSE 8080
# Expose Metrics port
EXPOSE 2112

# Use non-root user (distroless default)
USER nonroot:nonroot

ENTRYPOINT ["./mqueue"]