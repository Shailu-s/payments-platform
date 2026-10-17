FROM golang:1.26.7-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/mock-network ./cmd/mock-network

FROM alpine:3.20
COPY --from=build /out/mock-network /usr/local/bin/mock-network
EXPOSE 8082
ENTRYPOINT ["/usr/local/bin/mock-network"]
