FROM golang:1.25 AS builder
    WORKDIR /app
    COPY . .
    RUN make build
    RUN useradd -u 10001 tsdb-aggregate-proxy

    # Stage 2: Create the final, minimal image
    FROM busybox:stable-glibc
    WORKDIR /
    COPY --from=builder /app/bin/tsdb-aggregate-proxy .
    COPY --from=builder /etc/passwd /etc/passwd
    USER tsdb-aggregate-proxy
    ENTRYPOINT ["/tsdb-aggregate-proxy"]