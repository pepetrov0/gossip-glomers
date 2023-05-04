#!/bin/sh

cargo build

./maelstrom test -w broadcast --bin target/debug/fault-tolerant-broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition