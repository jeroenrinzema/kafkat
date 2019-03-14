FROM golang:1.12 as builder
WORKDIR /tmp/go
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -o exec .

FROM scratch as production
COPY --from=builder /tmp/go/exec .
CMD ["./exec"]
