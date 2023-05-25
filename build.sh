#!/usr/bin/env bash

function fail() {
    local error="$*" || 'Unknown error'
    echo "$(chalk red "${error}")"
    exit 1
}

function build() {
    local connector="$1"
    if [[ $2 == "driver" ]]; then
        path=drivers/$connector
    elif [[ $2 == "adapter" ]]; then
        path=adapters/$connector
    else
        fail "The argument does not have a recognized prefix."
    fi
    cd $path
    go mod tidy
    go build -ldflags="-w -s -X constants/constants.version=${GIT_VERSION} -X constants/constants.commitsha=${GIT_COMMITSHA} -X constants/constants.releasechannel=${RELEASE_CHANNEL}" -o syndicate main.go
    mv syndicate ../../
}

if [ $# -gt 0 ]; then
    argument="$1"

    if [[ $argument == driver-* ]]; then
        driver="${argument#driver-}"
        echo "Building driver: $driver"
        build $driver "driver"
    elif [[ $argument == adapter-* ]]; then
        adapter="${argument#adapter-}"
        echo "Building adapter: $adapter"
        build $adapter "adapter"
    else
        fail "The argument does not have a recognized prefix."
    fi
else
    fail "No arguments provided."
fi
