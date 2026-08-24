#!/usr/bin/env bash
set -euo pipefail

docker build --file benzhi.Dockerfile --tag streamengine:benzhi .
