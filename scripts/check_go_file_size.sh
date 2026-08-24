#!/bin/sh

set -eu

limit="${GO_FILE_LINE_LIMIT:-500}"
failed=0

for file in $(find . -type f -name '*.go' -not -path './vendor/*' | sort); do
    lines=$(wc -l < "$file" | tr -d ' ')
    if [ "$lines" -gt "$limit" ]; then
        echo "$file has $lines lines; limit is $limit" >&2
        failed=1
    fi
done

exit "$failed"
