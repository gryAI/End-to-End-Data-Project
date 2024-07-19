# Libraries
import re
import pandas as pd
from sqlalchemy import create_engine, Engine
from sqlalchemy import URL
from rapidfuzz import fuzz
from fastapi import FastAPI, Request
from fastapi.responses import ORJSONResponse
import configparser
from fastapi import FastAPI


# FastAPI app
screening_api = FastAPI()


# Helper Functions
def clean_table(df: pd.DataFrame) -> pd.DataFrame:
    # Standardize names on the source table
    df['sdn_name_cleaned'] = df['sdn_name'].str.replace(r'[/\-]', ' ', regex=True)
    df['sdn_name_cleaned'] = df['sdn_name_cleaned'].str.replace(r'[^a-zA-Z0-9]', ' ', regex=True)
    df['sdn_name_cleaned'] = df['sdn_name_cleaned'].str.replace(r'\s+', ' ', regex=True)
    df['sdn_name_cleaned'] = df['sdn_name_cleaned'].str.strip()
    df['sdn_name_cleaned'] = df['sdn_name_cleaned'].str.upper()

    return df


def get_consolidated_sanctions() -> pd.DataFrame:
    # Get config variables    
    config = configparser.ConfigParser()
    config.read('secrets.ini')
    DB_HOST = config['DATABASE CREDENTIALS']['DBHOST']
    DB_PORT = config['DATABASE CREDENTIALS']['DBPORT']
    DB_USER = config['DATABASE CREDENTIALS']['DBUSER']
    DB_PASS = config['DATABASE CREDENTIALS']['DBPASS']
    DB_NAME = config['DATABASE CREDENTIALS']['DBNAME']

    # Define connection string to PostgreSQL database
    connection_string = URL.create(
        drivername="postgresql+psycopg2",
        database = DB_NAME,
        host = DB_HOST,
        port = DB_PORT,
        username = DB_USER,
        password = DB_PASS
    )

    # Create engine object
    engine = create_engine(url=connection_string)

    # Load table from SQL
    df = pd.read_sql("SELECT * FROM ofac_consolidated", con = engine)
    df = clean_table(df)

    return df


def standardize_name(name: str) -> str:
    clean_name = re.sub("[/-]", " ", name).upper()
    clean_name = re.sub("[^A-Z0-9\\s]", "", clean_name)
    clean_name = re.sub("\\s+", " ", clean_name).strip()
    
    return clean_name


def get_ratio(s1: str, s2: str, sort_names: bool = True) -> float | None:
    # Sort names if specified
    if sort_names:
        s1 = " ".join(sorted(s1.split(" ")))
        s2 = " ".join(sorted(s2.split(" ")))

    # Return None on error
    try: 
        return round(fuzz.ratio(s1, s2) / 100, 4)
    
    except:
        return None


# Routes of FastAPI
@screening_api.get("/")
async def root():

    response = {
        "Status": "Success!",
        "Result": {
            "App Title": "Simple Screening API",
            "Version": "0.0.1"
        }
    }

    return response 


@screening_api.get("/screen")
async def screen(name: str, threshold: float = .7):

    cleaned_name = standardize_name(name)
    sanctions = get_consolidated_sanctions()

    # Screen the name and filter matches
    sanctions["similarity_score"] = sanctions["sdn_name_cleaned"].apply(
        get_ratio, args=(cleaned_name,)
    )
    
    sanctions_matched = sanctions[sanctions["similarity_score"] >= threshold]
    sanctions_matched.fillna("-", inplace = True)

    if len(sanctions_matched) > 0:
        response = {
            "Status": "Success!",
            "Response": sanctions_matched.to_dict(orient = "records")
            }
        
    else:
        response = {
    "Status": "Success!",
    "Response": f"No matches found for {cleaned_name}!"
    }
    
    return response
