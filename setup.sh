#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# solana-harpoon server setup
# Tested on: Ubuntu 24.04 (Hetzner Cloud / Dedicated)
#
# Usage (as root):
#   bash setup.sh                — uses default user 'ivan'
#   bash setup.sh --user ivan    — explicit user name
#
# After setup, log out and back in. cargo and harpoon will work immediately.
# =============================================================================

TARGET_USER="ivan"

while [[ $# -gt 0 ]]; do
  case $1 in
    --user) TARGET_USER="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

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

echo "=== Setup user: $TARGET_USER ==="
if ! id -u "$TARGET_USER" &>/dev/null; then
  useradd -m -s /bin/bash "$TARGET_USER"
  echo "Created user $TARGET_USER"
fi
echo "$TARGET_USER ALL=(ALL) NOPASSWD:ALL" > "/etc/sudoers.d/$TARGET_USER"

echo "=== Install Rust (as $TARGET_USER) ==="
su - "$TARGET_USER" -c '
  if ! command -v rustup &>/dev/null; then
    curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain none
  fi
  source "$HOME/.cargo/env"

  rustup install 1.88.0
  rustup default 1.88.0
  rustup component add clippy rustfmt

  echo "Rust version: $(rustc --version)"
  echo "Cargo version: $(cargo --version)"
'

# ── Fix: ensure cargo is in PATH for all shell types ──
# rustup adds to .bashrc but inside a guard that can fail.
# We source .cargo/env in both .bashrc and .profile.
BASHRC="/home/$TARGET_USER/.bashrc"
PROFILE="/home/$TARGET_USER/.profile"
CARGO_LINE='. "$HOME/.cargo/env"'

for RC_FILE in "$BASHRC" "$PROFILE"; do
  if ! grep -qF '.cargo/env' "$RC_FILE" 2>/dev/null; then
    printf '\n# Rust/Cargo PATH\n%s\n' "$CARGO_LINE" >> "$RC_FILE"
    echo "Added cargo PATH to $RC_FILE"
  fi
done

# ── Aliases: harpoon works from anywhere ──
MARKER="# --- solana-harpoon aliases ---"
if ! grep -qF "$MARKER" "$BASHRC" 2>/dev/null; then
  cat >> "$BASHRC" << 'EOF'

# --- solana-harpoon aliases ---
export HARPOON_HOME="$HOME/solana-harpoon"
alias harpoon='$HARPOON_HOME/target/release/harpoon'
alias harpoon-build='cd $HARPOON_HOME && cargo build --release'
alias harpoon-test='cd $HARPOON_HOME && cargo test --workspace'
EOF
  echo "Added harpoon aliases to $BASHRC"
fi

# Fix ownership in case we wrote as root
chown "$TARGET_USER:$TARGET_USER" "$BASHRC" "$PROFILE"

echo "=== Install Node.js + Claude Code (optional) ==="
if ! command -v node &>/dev/null; then
  curl -fsSL https://deb.nodesource.com/setup_22.x | bash -
  apt-get install -y nodejs
fi
# Uncomment when you have an Anthropic API key:
# su - "$TARGET_USER" -c 'npm install -g @anthropic-ai/claude-code'

echo "=== Setup data directories ==="
su - "$TARGET_USER" -c '
  mkdir -p ~/solana-harpoon
  mkdir -p ~/data
  mkdir -p ~/data/raw
  mkdir -p ~/data/output
'

echo "=== Configure system for large files ==="
cat > "/etc/security/limits.d/$TARGET_USER.conf" << EOF
$TARGET_USER soft nofile 65536
$TARGET_USER hard nofile 65536
EOF

echo "vm.max_map_count=262144" > /etc/sysctl.d/99-harpoon.conf
sysctl -p /etc/sysctl.d/99-harpoon.conf

echo "=== Setup swap (4GB) ==="
if [ ! -f /swapfile ]; then
  fallocate -l 4G /swapfile
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  echo '/swapfile none swap sw 0 0' >> /etc/fstab
  echo "Swap created: 4GB"
fi

echo ""
echo "=== System info ==="
echo "CPU:    $(nproc) cores"
echo "RAM:    $(free -h | awk '/Mem:/ {print $2}')"
echo "Swap:   $(free -h | awk '/Swap:/ {print $2}')"
echo "Disk:   $(df -h / | awk 'NR==2 {print $4}') free"
echo "User:   $TARGET_USER"

echo ""
echo "=== Done! ==="
echo ""
echo ">>> Log out and back in for PATH to work <<<"
echo ""
echo "  ssh $TARGET_USER@<server-ip>"
echo ""
echo "Verify:"
echo "  cargo --version"
echo "  rustc --version"
echo ""
echo "Build:"
echo "  cd ~/solana-harpoon"
echo "  git clone <your-repo> ."
echo "  cargo build --release"
echo ""
echo "After build, use 'harpoon' from anywhere:"
echo "  harpoon inspect --epoch 1"
echo "  harpoon ingest --epochs 700 \\"
echo "    --program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P \\"
echo "    --extract events --output ~/data/output --stream-download"
echo ""
echo "Shortcuts:"
echo "  harpoon-build    cargo build --release"
echo "  harpoon-test     cargo test --workspace"