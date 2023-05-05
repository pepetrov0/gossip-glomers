#!/bin/sh

cargo build

./maelstrom test -w g-counter --bin target/debug/grow-only-counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition