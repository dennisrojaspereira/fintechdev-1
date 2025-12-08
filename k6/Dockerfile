FROM golang:1.22 AS builder
ENV GOTOOLCHAIN=auto
RUN go install go.k6.io/xk6/cmd/xk6@latest
RUN xk6 build v0.55.0 \
    --with github.com/grafana/xk6-output-prometheus-remote@latest \
    -o /tmp/k6

FROM grafana/k6:0.55.0
COPY --from=builder /tmp/k6 /usr/bin/k6
ENTRYPOINT ["k6"]
