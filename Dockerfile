FROM golang:1.21-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -ldflags "-s -w" -o clicks3 .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /build/clicks3 /usr/local/bin/clicks3
ENTRYPOINT ["clicks3"]
