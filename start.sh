#!/usr/bin/env bash
set -euo pipefail

echo "==> Updating system..."
sudo apt update && sudo apt -y upgrade

echo "==> Installing base packages..."
sudo apt install -y \
  build-essential pkg-config cmake \
  git curl unzip \
  ca-certificates \
  lz4 zstd \
  jq htop iotop iftop \
  clang

echo "==> Installing Rust via rustup..."
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

echo "==> Enabling cargo env for current shell..."
# shellcheck disable=SC1090
source "$HOME/.cargo/env"

echo "==> Installing aria2..."
sudo apt install -y aria2

echo "==> Done."
echo "Rust:  $(rustc --version)"
echo "Cargo: $(cargo --version)"
echo "aria2: $(aria2c --version | head -n 1)"