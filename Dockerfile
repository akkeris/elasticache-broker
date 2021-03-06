FROM golang:1.14-alpine
ENV GO111MODULE=on
RUN apk update
RUN apk add openssl ca-certificates git make build-base postgresql
RUN wget https://github.com/golang/dep/releases/download/v0.5.0/dep-linux-amd64 -O /usr/bin/dep
RUN chmod +x /usr/bin/dep
WORKDIR /go/src/github.com/akkeris/elasticache-broker
COPY . .
RUN make
CMD ./servicebroker -insecure -logtostderr=1 -stderrthreshold 0 
