FROM golang:alpine

WORKDIR /opt/app

COPY ./go-trader/go-trader /opt/app/go-trader
COPY ./go-trader/go-trader/config.json /opt/app/config.json

EXPOSE 5244

CMD ["/opt/app/go-trader"]