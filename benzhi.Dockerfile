FROM golang:1.23-bookworm

ENV GOPROXY=off
ENV GOSUMDB=off
ENV CGO_ENABLED=0

WORKDIR /app

COPY go.mod go.sum ./
COPY vendor ./vendor
COPY . .

RUN go build -mod=vendor -o /out/streamengine ./cmd/streamengine

EXPOSE 8080

CMD ["/out/streamengine", "-addr", ":8080", "-web", "/app/web"]
