FROM golang:1.12 as builder
WORKDIR /tmp/go
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -o exec .
RUN mv exec /bin/kafkat
