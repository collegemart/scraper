#!/bin/bash
# ==============================================================================
# BharatFare Scraper - Ubuntu VM Setup Script
# Run this script on a fresh Ubuntu Server (Azure, DigitalOcean, AWS, etc.)
# ==============================================================================

echo "🚀 Starting setup for BharatFare Scraper on Ubuntu..."

# 1. Update system packages
echo "📦 Updating system packages..."
sudo apt update && sudo apt upgrade -y

# 2. Install Python, pip, and essential tools (tmux for keeping it running in background)
echo "🐍 Installing Python 3, pip, and background tools..."
sudo apt install -y python3 python3-pip python3-venv git tmux htop software-properties-common

# 3. Create and activate virtual environment
echo "🌐 Creating Python virtual environment..."
python3 -m venv .venv
source .venv/bin/activate

# 4. Install Python dependencies
echo "📚 Installing Python libraries from requirements.txt..."
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt

# 5. Install Playwright browser and OS dependencies
echo "🌐 Installing Chromium browser for Playwright..."
playwright install chromium
sudo playwright install-deps chromium

echo "=========================================================================="
echo "✅ SETUP COMPLETE!"
echo ""
echo "To run the scraper securely in the background (so it won't stop when you close your laptop):"
echo "  1. Type: tmux"
echo "  2. Type: source .venv/bin/activate"
echo "  3. Type: python run_master.py"
echo ""
echo "To detach and leave it running: Press Ctrl+B, then press D"
echo "To check on it later: Type 'tmux attach'"
echo "=========================================================================="
