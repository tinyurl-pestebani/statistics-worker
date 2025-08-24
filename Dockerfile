FROM golang:1.24.6-alpine3.22 AS builder
LABEL authors="pablo"

# Create a non-root user and group
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /src

COPY "./cmd" "./cmd"
COPY ./pkg ./pkg
COPY go.mod go.mod

RUN go mod tidy && CGO_ENABLED=0 go build -ldflags="-s -w" -o /main cmd/worker/main.go


FROM scratch

COPY --from=builder /main /main

# Copy the user and group files from the builder stage
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group

# Set the non-root user
USER appuser:appgroup

ENTRYPOINT ["/main"]
