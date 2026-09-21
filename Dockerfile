FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -o /overpass ./cmd/overpass

FROM alpine:3.20
COPY --from=build /overpass /overpass
COPY overpass.json /overpass.json
EXPOSE 8080 9090
ENTRYPOINT ["/overpass", "-config", "/overpass.json"]
