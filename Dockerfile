# Build stage
FROM golang:alpine AS builder

ENV GOTOOLCHAIN=auto

WORKDIR /app

# Copy dependency specifications first for layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source tree
COPY . .

# Build statically linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /kozlony ./cmd/kozlony

# Runtime stage
FROM alpine:3.21

RUN apk --no-cache add ca-certificates tzdata && \
    addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /home/appuser

COPY --from=builder /kozlony /usr/local/bin/kozlony

USER appuser

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/kozlony"]
