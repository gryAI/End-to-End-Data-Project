import sys
import json
import time
import schedule
import configparser
import pandas as pd
from os import environ, remove
from pathlib import Path
from ftplib import FTP_TLS


def get_ftp() -> FTP_TLS:
    # Get credentials from config file
    config = configparser.ConfigParser()
    config.read('secrets.ini')

    # Get FTP details
    FTPHOST = config['FTP CREDENTIALS']['FTPHOST']
    FTPUSER = config['FTP CREDENTIALS']['FTPUSER']
    FTPPASS = config['FTP CREDENTIALS']['FTPPASS']

    # Return authenticated FTP
    ftp = FTP_TLS(FTPHOST, FTPUSER, FTPPASS)
    ftp.prot_p()

    return ftp


def read_csv(config: dict) -> pd.DataFrame:
    url = config['URL']
    params = config['PARAMS']

    return pd.read_csv(url, **params)


def upload_to_ftp(ftp: FTP_TLS, file_source: str | Path):
    with open(file_source, "rb") as fp:
        ftp.storbinary(f"STOR {file_source.name}", fp)


def delete_file(file_source: str | Path):
    remove(file_source)


def pipeline():
    
    ftp = get_ftp()
    
    # Load source configuration
    with open('config.json', 'rb') as fp:
        config = json.load(fp)

    # Loop through each source url and configuration to get the source_name and its corresponding configuration
    count = 0
    for source_name, source_config in config.items():
        count += 1 
        file_name = Path(source_name + ".CSV")
        df = read_csv(source_config)
        df.to_csv(file_name, index = False)

        print(f"{file_name} has been downloaded")


        upload_to_ftp(ftp, file_name)
        print(f"{file_name} has been uploaded to FTP Server")


        delete_file(file_name)
        print(f"{file_name} has been deleted locally")

    print(f"SCHEDULED BATCH COMPLETED! File count: {count}")


if __name__=="__main__":

    param = sys.argv[1]

    if param == 'manual':
        pipeline()
    
    elif param == 'scheduled':

        schedule.every().day.at("17:39").do(pipeline)
        
        while True:
            schedule.run_pending()
            time.sleep(1)

    else:
        print("Invalid parameter. The app will not run. Please indicate whether run is manual or scheduled.")