#!/usr/bin/env bash
RUSTFLAGS="-Ctarget-cpu=native"
docker run -v $PWD:/volume --rm -t clux/muslrust cargo build --release
docker build . --no-cache --tag stor.highloadcup.ru/accounts/quick_giraffe
docker push stor.highloadcup.ru/accounts/quick_giraffe
