import requests
import pandas as pd
import json
from typing import List, Dict, Any
import logging
import time
import os
from io import StringIO

class StatDanmarkAPI:
    def __init__(self):
        self.base_url = "https://api.statbank.dk/v1"
        self.logger = self._setup_logger()
        self.output_dir = "/Users/lenna/Desktop/bdp"

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger('StatDanmark')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def fetch_crime_data(self, table_id: str = "STRAF11") -> pd.DataFrame:
        """Fetch crime data for all of Denmark"""
        payload = {
            "table": table_id,
            "format": "CSV",
            "lang": "en",
            "variables": [
                {
                    "code": "OMRÅDE",     
                    "values": ["*"]       
                },
                {
                    "code": "OVERTRÆD",   
                    "values": ["*"]       
                },
                {
                    "code": "Tid",        
                    "values": [           
                        "2024K3", "2024K2", "2024K1",
                        "2023K4", "2023K3", "2023K2", "2023K1",
                        "2022K4", "2022K3", "2022K2", "2022K1",
                        "2021K4", "2021K3", "2021K2", "2021K1",
                        "2020K4", "2020K3", "2020K2", "2020K1"
                    ]
                }
            ]
        }

        try:
            self.logger.info("Sending request to StatDanmark API...")
            response = requests.post(f"{self.base_url}/data", json=payload)
            
            if response.status_code == 200:
                self.logger.info("Successfully received data from API")
                content = response.content.decode('utf-8')
                
                self.logger.info("First few lines of response:")
                first_lines = content.split("\n")[:5]
                self.logger.info("\n".join(first_lines))
                
                df = pd.read_csv(StringIO(content), sep=';')
                self.logger.info(f"Created DataFrame with shape: {df.shape}")
                return df
            else:
                self.logger.error(f"Failed to fetch data: {response.status_code}")
                self.logger.error(f"Response content: {response.content.decode('utf-8')}")
                return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error fetching crime data: {str(e)}")
            return pd.DataFrame()

    def get_table_metadata(self, table_id: str = "STRAF11") -> Dict[str, Any]:
        """Get metadata for the crime statistics table"""
        payload = {
            "table": table_id,
            "format": "json",
            "lang": "en"
        }
        
        try:
            response = requests.post(f"{self.base_url}/tableinfo", json=payload)
            if response.status_code == 200:
                metadata = response.json()
                if 'variables' in metadata:
                    self.logger.info("Available variables:")
                    for var in metadata['variables']:
                        self.logger.info(f"Code: {var.get('id')} - Text: {var.get('text')}")
                return metadata
            return {}
        except Exception as e:
            self.logger.error(f"Error fetching metadata: {str(e)}")
            return {}

    def save_to_csv(self, df: pd.DataFrame, filename: str) -> bool:
        """Save DataFrame to CSV with error handling"""
        try:
            filepath = os.path.join(self.output_dir, filename)
            
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            
            if os.path.exists(filepath):
                file_size = os.path.getsize(filepath)
                self.logger.info(f"File saved successfully at: {filepath}")
                self.logger.info(f"File size: {file_size} bytes")
                return True
            return False
        except Exception as e:
            self.logger.error(f"Error saving CSV file: {str(e)}")
            return False

def main():
    api = StatDanmarkAPI()
    
    metadata = api.get_table_metadata()
    print("Fetching crime statistics from StatDanmark...")
    print(f"Table description: {metadata.get('text', 'No description available')}")

    df = api.fetch_crime_data()
    
    if not df.empty:
        print("\nData retrieved successfully!")
        print(f"DataFrame shape: {df.shape}")
        
        if api.save_to_csv(df, "denmark_crime_statistics.csv"):
            print("\nData has been saved to CSV successfully!")
            print(f"File location: {os.path.join(api.output_dir, 'denmark_crime_statistics.csv')}")
        else:
            print("\nFailed to save data to CSV!")
        
    else:
        print("No data was retrieved. Please check the logs for errors.")

if __name__ == "__main__":
    main()