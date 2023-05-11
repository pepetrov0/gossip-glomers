#!/bin/sh

cargo build

./maelstrom test -w kafka --bin target/debug/single-node-kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 1000