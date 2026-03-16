#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# solana-harpoon server setup
# Tested on: Ubuntu 24.04 (Hetzner Cloud / Dedicated)
#
# Usage:
#   scp setup.sh root@<server-ip>:~
#   ssh root@<server-ip> bash setup.sh
# =============================================================================

echo "=== System update ==="
apt-get update && apt-get upgrade -y

echo "=== Install build dependencies ==="
apt-get install -y \
  build-essential \
  pkg-config \
  libssl-dev \
  protobuf-compiler \
  aria2 \
  git \
  curl \
  htop \
  tmux \
  zstd \
  cmake \
  clang

echo "=== Create working user ==="
if ! id -u harpoon &>/dev/null; then
  useradd -m -s /bin/bash harpoon
  echo "harpoon ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/harpoon
fi

echo "=== Install Rust (as harpoon user) ==="
su - harpoon -c '
  if ! command -v rustup &>/dev/null; then
    curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain none
  fi
  source "$HOME/.cargo/env"
  
  # rust-toolchain.toml will handle the exact version,
  # but install 1.88.0 as default so cargo works immediately
  rustup install 1.88.0
  rustup default 1.88.0
  rustup component add clippy rustfmt

  echo "Rust version: $(rustc --version)"
  echo "Cargo version: $(cargo --version)"
'

echo "=== Install Claude Code (optional) ==="
if ! command -v node &>/dev/null; then
  curl -fsSL https://deb.nodesource.com/setup_22.x | bash -
  apt-get install -y nodejs
fi
# Claude Code CLI — uncomment if you have API key
# su - harpoon -c 'npm install -g @anthropic-ai/claude-code'

echo "=== Setup data directories ==="
su - harpoon -c '
  mkdir -p ~/solana-harpoon
  mkdir -p ~/data          # CAR downloads + parquet output
  mkdir -p ~/data/raw      # downloaded CARs (temporary)
  mkdir -p ~/data/output   # parquet output (keep)
'

echo "=== Configure system for large files ==="
# Increase max open files (needed for mmap of large CARs)
cat > /etc/security/limits.d/harpoon.conf << 'EOF'
harpoon soft nofile 65536
harpoon hard nofile 65536
EOF

# Increase vm.max_map_count for mmap
echo "vm.max_map_count=262144" > /etc/sysctl.d/99-harpoon.conf
sysctl -p /etc/sysctl.d/99-harpoon.conf

echo "=== Setup swap (for cloud instances with limited RAM) ==="
if [ ! -f /swapfile ]; then
  fallocate -l 4G /swapfile
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  echo '/swapfile none swap sw 0 0' >> /etc/fstab
  echo "Swap created: 4GB"
fi

echo "=== Print system info ==="
echo "CPU:    $(nproc) cores"
echo "RAM:    $(free -h | awk '/Mem:/ {print $2}')"
echo "Swap:   $(free -h | awk '/Swap:/ {print $2}')"
echo "Disk:   $(df -h / | awk 'NR==2 {print $4}') free"
echo "OS:     $(lsb_release -ds 2>/dev/null || cat /etc/os-release | grep PRETTY_NAME | cut -d= -f2)"

echo ""
echo "=== Done! ==="
echo ""
echo "Next steps:"
echo "  1. ssh harpoon@<server-ip>"
echo "  2. cd ~/solana-harpoon"
echo "  3. git clone <your-repo> .    # or scp your code"
echo "  4. cargo build --release"
echo "  5. cargo test --workspace"
echo ""
echo "Quick test (small epoch):"
echo "  ./target/release/harpoon ingest --epochs 1 \\"
echo "    --program '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' \\"
echo "    --output ~/data/output"
echo ""
echo "Full epoch test (in tmux so it survives disconnect):"
echo "  tmux new -s harpoon"
echo "  ./target/release/harpoon ingest --epochs 880 \\"
echo "    --program '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' \\"
echo "    --idl ./idls/6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P.json \\"
echo "    --output ~/data/output"
echo "  # Ctrl+B, D to detach; tmux attach -t harpoon to reattach"
