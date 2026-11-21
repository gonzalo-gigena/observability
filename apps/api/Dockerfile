# syntax=docker/dockerfile:1
# Compile on the build machine's own architecture and cross-compile to the target,
# so arm64 images build fast even on amd64 CI (no QEMU for the Go compiler).
FROM --platform=$BUILDPLATFORM golang:1.27.4-bookworm AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/api .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/api /app/api
EXPOSE 8080
ENTRYPOINT ["/app/api"]
