#!/usr/bin/env bash
docker run -v $PWD:/volume --rm -t clux/muslrust cargo build
docker build . --no-cache --tag stor.highloadcup.ru/accounts/quick_giraffe
docker push stor.highloadcup.ru/accounts/quick_giraffe
