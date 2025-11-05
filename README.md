
# ✈️ Lufthansa Data Project

This repository contains my solution for the **Lufthansa Mini Data Project** .

The project implements a **Medallion Architecture** consisting of three main layers:
- 🥉 **Bronze Layer:** Raw data ingestion  
- 🥈 **Silver Layer:** Data cleaning and enrichment  
- 🥇 **Gold Layer:** Aggregation and KPI computation  
- 📊 **Reporting:** Analytical SQL-based insights

---

## 🚀 Getting Started

### 1. Clone the Repository
```bash

git clone https://github.com/Arbs28/lufthansa-data-project.git
cd lufthansa-data-project

```
### 2. Run Automatically
```bash
```bash

```
chmod +x run_pipeline.sh
./run_pipeline.sh

This script will:

Create a Python virtual environment

Activate it

Install dependencies from requirements.txt

Execute all layers sequentially (Bronze → Silver → Gold → Reporting)
```
```
### 3. Run Manually 
```bash
```bash

python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt

# 1️⃣ Bronze Layer – Raw data ingestion
python3 -m src.01_bronze_ingestion

# 2️⃣ Silver Layer – Data cleaning and enrichment
python3 -m src.02_silver_transformations

# 3️⃣ Gold Layer – KPI and analytics creation
python3 -m src.03_gold_analytics

# 4️⃣ Reporting Layer – Analytical SQL queries
python3 -m src.04_reporting_queries
```
```
```
```
```
```
```
```
### 4. Run Jupyter Notebooks
```bash

jupyter notebook

# Which will open a UI allowing to run each notebook
```
```
