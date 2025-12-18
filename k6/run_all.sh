#!/bin/sh
set -e

for svc in go:8080 java:8080 node:3000 python:8000 php:8000 dotnet:8080; do
  echo "Testing $svc"
  BASE_URL="http://$svc" k6 run --vus 5 --duration 5s loadtest.js
done