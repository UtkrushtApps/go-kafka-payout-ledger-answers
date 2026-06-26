FROM golang:1.22-alpine AS build

WORKDIR /root/task
COPY go.mod ./
RUN go mod download
COPY . .
RUN go build -mod=mod -o /out/payout-ledger ./cmd/worker

FROM alpine:3.20
WORKDIR /app
COPY --from=build /out/payout-ledger /app/payout-ledger
CMD ["/app/payout-ledger"]
