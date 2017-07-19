#!/bin/sh
export INDEX_VERSION=$(cat ./mapping.version)

exec ./elasticsearch-reindexer
