# Electricity Load ETL

## Overview  
This project implements a full **ETL (Extract–Transform–Load) pipeline** for electricity load auction data in the **PJM region**.  
It automatically downloads data from PJM regional utilities’ websites, validates and cleans it, and loads the results into structured database tables.  

The pipeline also fetches **PJM reference data** such as **NSPL, NITS, and PLC Scaling factors** to cross-check against utility-provided data.  
In addition, it generates standardized **data quality reports** to ensure integrity before loading.

---

## Features  
- 🔄 **Automated Data Ingestion**  
  - Separate Python scripts for each regional utility’s website  
  - Dedicated `pjm/` module for PJM system-level data (NSPL, NITS, PLCScaling)  
  - Supports scheduled runs for daily/weekly updates  

- ✅ **Data Validation & Quality Control**  
  - Detects missing values, duplicates, and anomalies  
  - Cross-checks utility load data against PJM reference factors  
  - Generates standardized QC reports  

- 🗄 **Database Integration**  
  - Loads cleaned data into SQL database tables  
  - Supports incremental updates and historical backfill  

- 📊 **Reporting**  
  - Produces automated reports (coverage, anomalies, summary stats)  
  - Includes templates for Excel/Word output  


## Workflow  

1. **Extraction**  
   - Run `utilities/<utility>_loader.py` to download auction load data  
   - Run `pjm/fetch_*.py` to fetch PJM reference factors  

2. **Transformation**  
   - Standardize timestamps (America/New_York)  
   - Normalize schema across utilities  
   - Validate against PJM reference data  

3. **Loading**  
   - Insert validated results into SQL tables (`Load_DailyVolumeHist`, `NSPL_Factors`, etc.)  
   - Supports incremental and full loads  

4. **Reporting**  
   - Run report generator to produce QC reports  
   - Reports highlight anomalies, missing coverage, and mismatches  

---

## Quickstart  

Example: Run ETL for Ohio AEP
python utilities/

Example: Fetch PJM NSPL factors
python pjm/nspl.py

## Data Sources

PJM Data Miner 2 – https://dataminer2.pjm.com

Utility websites – AEP Ohio, Duke Ohio, PSEG NJ, etc.


