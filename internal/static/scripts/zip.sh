#!/bin/bash
set -eu

# Zip the dist directory into dist.zip
cd "$(dirname "$0")/.."

rm -f dist.zip
zip -r dist.zip dist/*
