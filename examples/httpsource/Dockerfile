FROM golang:1.26-bookworm as build
WORKDIR /base
ADD . /base/
RUN go build -o /base/app .
ENTRYPOINT ["/base/app"]