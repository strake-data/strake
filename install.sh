#!/bin/bash
# Strake Universal Installer
# Usage: curl -sSfL https://strakedata.com/install.sh | sh
#    or: curl -sSfL https://strakedata.com/install.sh | sh -s -- --all
#    or: curl -sSfL https://strakedata.com/install.sh | sh -s -- --cli --server

set -euo pipefail

REPO="strake-data/strake"
INSTALL_DIR="${STRAKE_INSTALL_DIR:-$HOME/.local/bin}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

# Binaries to install
INSTALL_CLI=false
INSTALL_SERVER=false
INSTALL_ENTERPRISE=false
VERSION=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --all|-a)
            INSTALL_CLI=true
            INSTALL_SERVER=true
            INSTALL_ENTERPRISE=true
            shift
            ;;
        --cli)
            INSTALL_CLI=true
            shift
            ;;
        --server)
            INSTALL_SERVER=true
            shift
            ;;
        --enterprise)
            INSTALL_ENTERPRISE=true
            shift
            ;;
        --version|-v)
            VERSION="$2"
            shift 2
            ;;
        --help|-h)
            cat <<EOF
Strake Universal Installer

Install the Strake data platform binaries.

Usage: 
  curl -sSfL https://strakedata.com/install.sh | sh [OPTIONS]

Options:
  --all, -a           Install all binaries (cli, server, enterprise)
  --cli               Install strake-cli only
  --server            Install strake-server only
  --enterprise        Install strake-enterprise only
  --version, -v       Install a specific version (e.g., v0.1.0)
  --help, -h          Show this help message

Environment Variables:
  STRAKE_INSTALL_DIR  Installation directory (default: ~/.local/bin)

Examples:
  # Install just the CLI
  curl -sSfL https://strakedata.com/install.sh | sh -s -- --cli

  # Install server and enterprise
  curl -sSfL https://strakedata.com/install.sh | sh -s -- --server --enterprise

  # Install everything
  curl -sSfL https://strakedata.com/install.sh | sh -s -- --all

  # Install a specific version
  curl -sSfL https://strakedata.com/install.sh | sh -s -- --cli --version v0.1.0

EOF
            exit 0
            ;;
        *)
            log_error "Unknown option: $1. Use --help for usage information."
            ;;
    esac
done

# If nothing specified, default to CLI only
if [[ "$INSTALL_CLI" == "false" && "$INSTALL_SERVER" == "false" && "$INSTALL_ENTERPRISE" == "false" ]]; then
    INSTALL_CLI=true
fi

# Detect OS and architecture
detect_platform() {
    local os arch

    case "$(uname -s)" in
        Linux*)  os="unknown-linux" ;;
        Darwin*) os="apple-darwin" ;;
        MINGW*|MSYS*|CYGWIN*) os="pc-windows-msvc" ;;
        *) log_error "Unsupported operating system: $(uname -s)" ;;
    esac

    case "$(uname -m)" in
        x86_64|amd64) arch="x86_64" ;;
        arm64|aarch64) arch="aarch64" ;;
        *) log_error "Unsupported architecture: $(uname -m)" ;;
    esac

    # Linux: prefer musl for better portability
    if [[ "$os" == "unknown-linux" ]]; then
        if command -v ldd >/dev/null 2>&1 && ldd --version 2>&1 | grep -q musl; then
            os="unknown-linux-musl"
        else
            os="unknown-linux-gnu"
        fi
    fi

    echo "${arch}-${os}"
}

# Get the latest release version from GitHub
get_latest_version() {
    local version
    version=$(curl -sSf "https://api.github.com/repos/${REPO}/releases/latest" | 
        grep '"tag_name":' | 
        sed -E 's/.*"([^"]+)".*/\1/')
    
    if [[ -z "$version" ]]; then
        log_error "Failed to determine latest version"
    fi
    
    echo "$version"
}

# Download and install a binary
install_binary() {
    local binary_name="$1"
    local platform="$2"
    local version="$3"
    local download_url="https://github.com/${REPO}/releases/download/${version}/${binary_name}-${platform}"
    local temp_file
    
    # Windows uses .exe extension
    if [[ "$platform" == *"windows"* ]]; then
        download_url="${download_url}.exe"
    fi

    temp_file=$(mktemp)
    trap 'rm -f "$temp_file"' EXIT

    log_info "Downloading ${binary_name} ${version} for ${platform}..."
    
    if ! curl -sSfL "$download_url" -o "$temp_file"; then
        log_error "Failed to download from ${download_url}"
    fi

    # Verify it's a valid binary (basic check)
    if [[ ! -s "$temp_file" ]]; then
        log_error "Downloaded file is empty"
    fi

    # Create install directory if it doesn't exist
    mkdir -p "$INSTALL_DIR"

    # Install the binary
    local install_path="${INSTALL_DIR}/${binary_name}"
    if [[ "$platform" == *"windows"* ]]; then
        install_path="${install_path}.exe"
    fi

    mv "$temp_file" "$install_path"
    chmod +x "$install_path"

    log_info "✓ Installed ${binary_name} to ${install_path}"
}

# Check if install directory is in PATH
check_path() {
    if [[ ":$PATH:" != *":$INSTALL_DIR:"* ]]; then
        echo ""
        log_warn "${INSTALL_DIR} is not in your PATH"
        echo ""
        echo "Add it to your shell configuration:"
        echo ""
        echo "  ${BLUE}# For bash (~/.bashrc)${NC}"
        echo "  export PATH=\"\$PATH:${INSTALL_DIR}\""
        echo ""
        echo "  ${BLUE}# For zsh (~/.zshrc)${NC}"
        echo "  export PATH=\"\$PATH:${INSTALL_DIR}\""
        echo ""
        echo "  ${BLUE}# For fish (~/.config/fish/config.fish)${NC}"
        echo "  set -gx PATH \$PATH ${INSTALL_DIR}"
        echo ""
    fi
}

main() {
    cat <<'EOF'
   _____ _             _        
  / ____| |           | |       
 | (___ | |_ _ __ __ _| | _____ 
  \___ \| __| '__/ _` | |/ / _ \
  ____) | |_| | | (_| |   <  __/
 |_____/ \__|_|  \__,_|_|\_\___|
                                
EOF

    log_info "Strake Universal Installer"
    echo ""

    local platform
    platform=$(detect_platform)
    log_info "Detected platform: ${platform}"

    if [[ -z "$VERSION" ]]; then
        VERSION=$(get_latest_version)
    fi
    log_info "Version: ${VERSION}"
    echo ""

    # Install selected binaries
    if [[ "$INSTALL_CLI" == "true" ]]; then
        install_binary "strake-cli" "$platform" "$VERSION"
    fi
    
    if [[ "$INSTALL_SERVER" == "true" ]]; then
        install_binary "strake-server" "$platform" "$VERSION"
    fi
    
    if [[ "$INSTALL_ENTERPRISE" == "true" ]]; then
        install_binary "strake-enterprise" "$platform" "$VERSION"
    fi

    check_path

    echo ""
    log_info "Installation complete!"
    echo ""
    echo "Next steps:"
    
    if [[ "$INSTALL_CLI" == "true" ]]; then
        echo "  • Run ${BLUE}strake-cli --help${NC} to get started with the CLI"
    fi
    
    if [[ "$INSTALL_SERVER" == "true" ]]; then
        echo "  • Run ${BLUE}strake-server --help${NC} to start the query server"
    fi
    
    if [[ "$INSTALL_ENTERPRISE" == "true" ]]; then
        echo "  • Run ${BLUE}strake-enterprise --help${NC} to start the enterprise server"
    fi
    
    echo ""
    echo "Documentation: ${BLUE}https://docs.strakedata.com${NC}"
}

main
