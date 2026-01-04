import requests
import pprint
import os
from dotenv import load_dotenv # 1. Import the loader
from supabase import create_client, Client # 2. Import Supabase
import pandas as pd
import numpy as np

# Load the .env file
load_dotenv()

# Fetch variables from the environment
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Initialize the Supabase client
def get_supabase_client():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    return create_client(url, key)

#1. Extract data from OpenDOSM API, in json format
def fetch_lfs_data():
    url = "https://api.data.gov.my/data-catalogue?id=lfs_month&limit=100" 
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None

data = fetch_lfs_data()

#2. Transforming the data, cleaning and checking data

def transform_lfs(raw_data):
    df = pd.DataFrame(raw_data) #load into dataframe

    df['date']=pd.to_datetime(df['date'])

    numeric_cols = ['lf', 'lf_employed', 'lf_unemployed', 'lf_outside', 'u_rate', 'p_rate', 'ep_ratio']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # --- DATA QUALITY CHECKS ---
    
    # Check A: Missing Values
    initial_count = len(df)
    df = df.dropna()
    if len(df) < initial_count:
        print(f"⚠️ Dropped {initial_count - len(df)} rows with null values.")

    # Check B: Logical Math (Allowing for small rounding errors)
    # Check if employed + unemployed matches total labour force
    df['lf_calc_diff'] = abs((df['lf_employed'] + df['lf_unemployed']) - df['lf'])
    # If the difference is more than 0.2 (accounting for rounding in '000s), it's a red flag
    bad_math = df[df['lf_calc_diff'] > 0.2]
    
    if not bad_math.empty:
        print(f"❌ Logic Error: {len(bad_math)} rows failed lf_employed + lf_unemployed == lf")

    # Check C: Boundary check for rates
    invalid_rates = df[(df['u_rate'] < 0) | (df['u_rate'] > 100)]
    if not invalid_rates.empty:
        print(f"❌ Boundary Error: Found {len(invalid_rates)} rows with impossible unemployment rates.")

    # --- FINAL CLEANUP ---
    # Drop our helper calculation column before sending to Supabase
    df = df.drop(columns=['lf_calc_diff'])
    
    # Convert date back to string for JSON serialization
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    
    return df.to_dict(orient='records')

#4. loading data

def load(clean_data):
        print(f"Loading {len(clean_data)}) rows to supabase.....")
        supabase = get_supabase_client()
        supabase.table("malaysia_labour_force").upsert(clean_data).execute()
        print("Upload settle! ")

#5 Orchestration

def run_pipeline():
    try:
        raw= fetch_lfs_data()
        clean= transform_lfs(raw)
        load(clean)
    except Exception as e:
        print(f"Pipeline failed: {e}")

if __name__ == "__main__":
    run_pipeline()






