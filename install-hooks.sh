#!/bin/bash

## ----------------------------------------------------------
# Install Git hooks for Spark Connect C++ Client
#
# This script installs pre-commit and pre-push hooks that ensure
# code quality and tests pass before commits and pushes.
# ----------------------------------------------------------

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}*****************************************${NC}"
echo -e "${BLUE}  Git Hooks Installation${NC}"
echo -e "${BLUE}*****************************************${NC}"
echo ""

if [ ! -d ".git" ]; then
    echo -e "${RED}Error: Not a git repository${NC}"
    echo "Please run this script from the root of your git repository."
    exit 1
fi

# --------------------------------------
# Check if hooks directory exists
# --------------------------------------
if [ ! -d "hooks" ]; then
    echo -e "${RED}Error: hooks/ directory not found${NC}"
    echo "Please ensure you have the hooks/ directory in your repository."
    exit 1
fi

mkdir -p .git/hooks

install_hook() {
    local hook_name=$1
    local source_file="hooks/$hook_name"
    local dest_file=".git/hooks/$hook_name"
    
    if [ ! -f "$source_file" ]; then
        echo -e "${RED}✗ $source_file not found${NC}"
        return 1
    fi
    
    # ----------------------------------------------------------
    # Backup existing hook if it exists
    # ----------------------------------------------------------
    if [ -f "$dest_file" ]; then
        echo -e "${YELLOW}  Backing up existing $hook_name to ${hook_name}.backup${NC}"
        mv "$dest_file" "${dest_file}.backup"
    fi
    
    cp "$source_file" "$dest_file"
    chmod +x "$dest_file"
    
    echo -e "${GREEN} Installed $hook_name${NC}"
    return 0
}

echo "Installing hooks..."
echo ""

HOOKS_INSTALLED=0

if install_hook "pre-commit"; then
    HOOKS_INSTALLED=$((HOOKS_INSTALLED + 1))
fi

if install_hook "pre-push"; then
    HOOKS_INSTALLED=$((HOOKS_INSTALLED + 1))
fi

echo ""
echo -e "${GREEN}*****************************************${NC}"
echo -e "${GREEN}  Installation Complete...${NC}"
echo -e "${GREEN}*****************************************${NC}"
echo ""
echo -e "Installed $HOOKS_INSTALLED hook(s):"
echo ""
echo -e "${BLUE}pre-commit:${NC}"
echo "  - Runs quick build checks before each commit"
echo "  - Ensures code compiles"
echo ""
echo -e "${BLUE}pre-push:${NC}"
echo "  - Runs full test suite before push"
echo "  - Ensures all tests pass"
echo ""
echo -e "${YELLOW}To bypass hooks (not recommended):${NC}"
echo "  git commit --no-verify  # Skip pre-commit"
echo "  git push --no-verify    # Skip pre-push"
echo ""
echo -e "${YELLOW}To uninstall hooks:${NC}"
echo "  rm .git/hooks/pre-commit .git/hooks/pre-push"
echo ""