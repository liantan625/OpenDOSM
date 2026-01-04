# 🇲🇾 Malaysia Labour Force ETL Pipeline

**Automated Data Pipeline from OpenDOSM to Supabase (PostgreSQL)**

![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![Supabase](https://img.shields.io/badge/Supabase-PostgreSQL-green)
![Status](https://img.shields.io/badge/Status-Active-success)

## 📋 Overview
This project is an automated **ETL (Extract, Transform, Load)** pipeline that tracks Malaysia's Labour Force statistics. It extracts raw data from the Malaysian government's open API ([OpenDOSM](https://developer.data.gov.my/)), cleans and validates the data using Python, and loads it into a **Supabase PostgreSQL** database for analysis.

I built this project to solve the problem of manual data retrieval and to create a "single source of truth" for historical unemployment trends.

## 🏗 Architecture
**Data Source (OpenDOSM API)** ➡️ **Python (Extraction & Cleaning)** ➡️ **Supabase (Storage)**

* **Extract:** Python script hits the `lfs_month` endpoint (handling rate limits).
* **Transform:**
    * Converts JSON response to Pandas DataFrame.
    * **Data Quality Checks:** Verifies that `Employed` + `Unemployed` ≈ `Total Labour Force`.
    * **Formatting:** Standardizes date formats for SQL compatibility.
* **Load:** Upserts clean data into PostgreSQL to ensure no duplicate records.

## 🛠 Tech Stack
* **Language:** Python (Pandas, Requests)
* **Database:** Supabase (PostgreSQL)
* **Environment Management:** `python-dotenv` for secure API key storage.

## 🚀 How to Run Locally

1.  **Clone the Repo**
    ```bash
    git clone [https://github.com/YOUR_USERNAME/malaysia-labour-etl.git](https://github.com/YOUR_USERNAME/malaysia-labour-etl.git)
    cd malaysia-labour-etl
    ```

2.  **Install Dependencies**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Setup Environment Variables**
    Create a `.env` file in the root folder:
    ```ini
    SUPABASE_URL="your_supabase_url"
    SUPABASE_KEY="your_supabase_service_role_key"
    ```

4.  **Run the Pipeline**
    ```bash
    python main.py
    ```

## 📊 Sample Data Insight (SQL)
After loading the data, I ran this query to identify the peak unemployment month during the pandemic:

```sql
SELECT date, u_rate 
FROM malaysia_labour_force 
WHERE u_rate > 4.5 
ORDER BY u_rate DESC;
