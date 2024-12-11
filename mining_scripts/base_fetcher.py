import requests
import pandas as pd
import logging
import time
from typing import Dict, List
from pathlib import Path
from io import StringIO

class BaseFetcher:
    def __init__(self):
        self.base_url = "https://api.statbank.dk/v1"
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        # Using existing directory structure
        self.output_dir = Path("csvs-to-process")
        self.output_dir.mkdir(exist_ok=True)

    def get_table_metadata(self, table_id: str) -> Dict:
        """Fetch metadata for a specific table"""
        try:
            response = requests.get(f"{self.base_url}/tableinfo/{table_id}")
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error(f"Failed to fetch metadata for {table_id}: {response.status_code}")
                return {}
        except Exception as e:
            self.logger.error(f"Error fetching metadata for {table_id}: {str(e)}")
            return {}

    def fetch_data(self, table_id: str, payload: Dict) -> pd.DataFrame:
        """Fetch data from the API"""
        try:
            self.logger.info(f"Fetching data for {table_id}")
            response = requests.post(f"{self.base_url}/data", json=payload)
            
            if response.status_code == 200:
                df = pd.read_csv(StringIO(response.text), delimiter=';')
                self.logger.info(f"Retrieved {len(df)} rows")
                return df
            else:
                self.logger.error(f"API request failed: {response.status_code}")
                self.logger.error(f"Error details: {response.text}")
                return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"Error fetching data: {str(e)}")
            return pd.DataFrame()

    def save_to_csv(self, df: pd.DataFrame, filename: str):
        """Save dataframe to CSV in the existing csvs-to-process directory"""
        try:
            output_path = self.output_dir / filename
            df.to_csv(output_path, index=False)
            self.logger.info(f"Saved data to {output_path}")
        except Exception as e:
            self.logger.error(f"Error saving CSV: {str(e)}") 