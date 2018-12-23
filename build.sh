#!/usr/bin/env bash
docker run -v $PWD:/volume --rm -t clux/muslrust cargo build --release
docker build . --no-cache --tag stor.highloadcup.ru/accounts/gentle_robin
docker push stor.highloadcup.ru/accounts/gentle_robin
