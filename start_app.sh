#!/bin/bash
set -e # Exit immediately if any command fails

PROJECT_ROOT=$(pwd)
VENV_DIR="$PROJECT_ROOT/venv"
REQUIREMENTS_FILE="$PROJECT_ROOT/requirements.txt"

echo "Creating Python virtual environment"
if [ ! -d "$VENV_DIR" ]; then
  python3 -m venv "$VENV_DIR"
  echo "Virtual environment created at $VENV_DIR"
else
  echo "Virtual environment already exists"
fi

echo "Activate virtual env"
source "$VENV_DIR/bin/activate"

echo "Installing dependencies"
pip install --upgrade pip
pip install -r "$REQUIREMENTS_FILE"
echo "Dependencies installed successfully"

echo "Running Bronze ingestion"
python3 -m src.01_bronze_ingestion
echo "Bronze ingestion completed"

echo "Running Silver transformations"
python3 -m src.02_silver_transformations
echo "Silver transformations completed"

echo "Running Gold analytics"
python3 -m src.03_gold_analytics
echo "Gold analytics completed"

echo "Running Reporting queries"
python3 -m src.04_reporting_queries
echo "Reporting queries completed"
