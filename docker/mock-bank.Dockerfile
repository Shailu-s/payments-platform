FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/mock-bank ./cmd/mock-bank

FROM alpine:3.20
COPY --from=build /out/mock-bank /usr/local/bin/mock-bank
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/mock-bank"]
