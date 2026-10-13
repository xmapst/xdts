#!/usr/bin/env bash
set -Eeuo pipefail

# Build reproducible Linux release artifacts. The first argument is the public
# version; the second is optional and defaults to the current Git revision.
ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
VERSION="${1:-}"
REVISION="${2:-}"

if [[ -z "${VERSION}" ]]; then
  echo "usage: $0 VERSION [REVISION]" >&2
  exit 2
fi
if [[ -z "${REVISION}" ]]; then
  REVISION="$(git -C "${ROOT_DIR}" rev-parse --short HEAD 2>/dev/null || printf '%s' unknown)"
fi

command -v go >/dev/null 2>&1 || { echo "go is required" >&2; exit 1; }
command -v npm >/dev/null 2>&1 || { echo "npm is required to embed the Manager UI" >&2; exit 1; }

BUILD_TAGS="codec.notfastpath"
LDFLAGS="-w -s -X github.com/xmapst/xdts/internal/buildinfo.version=${VERSION} -X github.com/xmapst/xdts/internal/buildinfo.revision=${REVISION}"
OUT_DIR="${ROOT_DIR}/bin/release/${VERSION}"
mkdir -p "${OUT_DIR}"

printf 'building frontend\n'
(
  cd "${ROOT_DIR}/web"
  npm ci --silent
  npm run build
)

for arch in amd64 arm64; do
  for command_name in manager node; do
    output="${OUT_DIR}/xdts-${command_name}-linux-${arch}"
    printf 'building %s\n' "${output}"
    (
      cd "${ROOT_DIR}"
      CGO_ENABLED=0 GOOS=linux GOARCH="${arch}" \
        go build -trimpath -tags "${BUILD_TAGS}" -ldflags "${LDFLAGS}" \
        -o "${output}" "./cmd/${command_name}"
    )
  done
done

(
  cd "${OUT_DIR}"
  sha256sum \
    xdts-manager-linux-amd64 \
    xdts-manager-linux-arm64 \
    xdts-node-linux-amd64 \
    xdts-node-linux-arm64 \
    > SHA256SUMS
)
printf 'release written to %s\n' "${OUT_DIR}"
