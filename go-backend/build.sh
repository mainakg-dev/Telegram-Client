#!/usr/bin/env bash
set -e

mkdir -p bin

echo "🔨 Building Go Backend Master Server..."
/usr/local/go/bin/go build -o bin/server main.go

echo "🔨 Building Go Backend Worker Node..."
/usr/local/go/bin/go build -o bin/worker worker/worker.go

echo "✅ Build Complete! Executables created in ./bin/"
echo "   - Main Server: ./bin/server"
echo "   - Worker Node: ./bin/worker"
