FROM alpine
MAINTAINER seb

ENV GOPATH=/root
RUN apk update && apk add go git libc-dev

COPY kafka-producer.go /
RUN go get github.com/Shopify/sarama github.com/google/uuid
RUN go build /kafka-producer.go
COPY start.sh /
RUN chmod 755 /start.sh

CMD exec /kafka-producer.go $@
