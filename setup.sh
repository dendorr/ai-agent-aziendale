#!/bin/bash
# Setup script for AI Agent Aziendale
# Tested on WSL2 Ubuntu 24.04
# Run from your home directory: bash setup.sh

set -e

echo "Starting setup..."

# System packages
echo "Installing system dependencies..."
sudo apt-get update -qq
sudo apt-get install -y \
    python3.12 \
    python3.12-venv \
    python3.12-dev \
    python3-pip \
    tesseract-ocr \
    tesseract-ocr-ita \
    tesseract-ocr-eng \
    libtesseract-dev \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    build-essential \
    git \
    curl \
    wget

# Ollama
if ! command -v ollama &> /dev/null; then
    echo "Installing Ollama..."
    curl -fsSL https://ollama.com/install.sh | sh
fi

if ! systemctl is-active --quiet ollama 2>/dev/null; then
    sudo systemctl enable ollama
    sudo systemctl start ollama
    sleep 3
fi

# Python virtualenv
echo "Setting up Python environment..."
VENV="$HOME/ai-env"

if [ ! -d "$VENV" ]; then
    python3.12 -m venv "$VENV"
fi

source "$VENV/bin/activate"
pip install --upgrade pip --quiet

# Python packages
echo "Installing Python packages..."
pip install \
    fastapi \
    "uvicorn[standard]" \
    python-multipart \
    requests \
    chromadb \
    openpyxl \
    pandas \
    tabulate \
    xlrd \
    PyMuPDF \
    pdfplumber \
    markitdown \
    python-pptx \
    python-docx \
    pytesseract \
    easyocr \
    Pillow \
    numpy \
    watchdog \
    sentence-transformers \
    onnxruntime

# Project directories
echo "Creating project structure..."
PROJECT="$HOME/ai-agent"

mkdir -p "$PROJECT/config"
mkdir -p "$PROJECT/scripts"
mkdir -p "$PROJECT/memory"
mkdir -p "$PROJECT/logs"
mkdir -p "$PROJECT/chroma/financial"
mkdir -p "$PROJECT/chroma/documents"
mkdir -p "$PROJECT/chroma/drawings"
mkdir -p "$PROJECT/markdown_cache"
mkdir -p "$PROJECT/data/financial"
mkdir -p "$PROJECT/data/documents"
mkdir -p "$PROJECT/data/drawings"

deactivate

echo ""
echo "Done. Next steps:"
echo ""
echo "  1. Start Docker Desktop on Windows"
echo ""
echo "  2. Start Open WebUI:"
echo "     docker start open-webui"
echo ""
echo "     First time only:"
echo "     docker run -d -p 3000:3000 --add-host=host.docker.internal:host-gateway -v open-webui:/app/backend/data --name open-webui ghcr.io/open-webui/open-webui:main"
echo ""
echo "  3. Pull Ollama models:"
echo "     ollama pull qwen2.5:7b"
echo "     ollama pull qwen3:0.6b"
echo "     ollama pull llama3.2"
echo ""
echo "  4. Start the servers:"
echo "     source ~/ai-env/bin/activate"
echo "     cd ~/ai-agent/scripts"
echo "     nohup python server.py > ~/ai-agent/logs/server.log 2>&1 &"
echo "     nohup python watcher.py > ~/ai-agent/logs/watcher.log 2>&1 &"