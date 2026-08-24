#!/bin/sh

set -eu

unformatted=$(gofmt -l .)
if [ -n "$unformatted" ]; then
    echo "gofmt required for:" >&2
    echo "$unformatted" >&2
    exit 1
fi

go vet ./...

coverage_file=$(mktemp "${TMPDIR:-/tmp}/redis-ease-coverage.XXXXXX")
trap 'rm -f "$coverage_file"' EXIT
go test -count=1 -coverprofile="$coverage_file" ./...
coverage=$(go tool cover -func="$coverage_file" | awk '/^total:/ { gsub("%", "", $3); print $3 }')
minimum_coverage="${MIN_COVERAGE:-80}"
if ! awk -v actual="$coverage" -v minimum="$minimum_coverage" 'BEGIN { exit !(actual + 0 >= minimum + 0) }'; then
    echo "coverage ${coverage}% is below ${minimum_coverage}%" >&2
    exit 1
fi

go test -race -count=1 ./...
./scripts/check_go_file_size.sh
