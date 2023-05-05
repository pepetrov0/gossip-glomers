#!/bin/sh

cargo build

./maelstrom test -w broadcast --bin target/debug/efficient-broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100 --nemesis partition
