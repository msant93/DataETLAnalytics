#!/usr/bin/env bash
# One-command setup for Linux / macOS.
# Creates an isolated virtual environment, installs pinned deps, and validates.
set -euo pipefail

PYTHON="${PYTHON:-python3}"
VENV_DIR="env"

echo "==> Checking Python version"
"$PYTHON" -c 'import sys; assert sys.version_info >= (3,9), "Python 3.9+ required"'

echo "==> Creating virtual environment in ./$VENV_DIR"
"$PYTHON" -m venv "$VENV_DIR"

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

echo "==> Upgrading pip and installing dependencies"
python -m pip install --upgrade pip >/dev/null
pip install -r requirements.txt

echo "==> Registering Jupyter kernel"
python -m ipykernel install --user --name local-data-env --display-name "Local Data Env" >/dev/null 2>&1 || true

echo "==> Validating installation"
python -m scripts.smoke_test

echo ""
echo "Setup complete. Activate with:  source $VENV_DIR/bin/activate"
echo "Then run:                       python -m etl.cli run-etl"
