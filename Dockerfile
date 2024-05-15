FROM golang:1.21-alpine as base

# Install dependencies
RUN apk add git make bash build-base lzo-dev

ADD . /home/app/
# ADD . /home/app/
WORKDIR /home/app/drivers/s3/

RUN gofmt -l -s -w .
RUN CGO_ENABLED=1 go build -o dsynk main.go
RUN mv dsynk /

FROM golang:1.21-alpine
COPY --from=base /dsynk /home/
ADD . /home
RUN apk add build-base lzo-dev
LABEL io.airbyte.version=2.0.24

LABEL io.airbyte.name=airbyte/source-mysql

WORKDIR /home
ENTRYPOINT [ "./dsynk" ]