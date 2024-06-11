FROM golang:1.21.6-alpine AS builder
WORKDIR /app
COPY /consumer .
RUN go mod tidy
RUN go build -o main .
FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/main .
EXPOSE 6062
CMD ["./main"]
