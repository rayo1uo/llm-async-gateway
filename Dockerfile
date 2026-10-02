FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/gateway ./cmd/gateway \
    && CGO_ENABLED=0 go build -o /out/mockupstream ./cmd/mockupstream

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget \
    && adduser -D -u 65532 app
COPY --from=build /out/gateway /usr/local/bin/gateway
COPY --from=build /out/mockupstream /usr/local/bin/mockupstream
USER 65532
EXPOSE 8080 8090
CMD ["gateway"]
