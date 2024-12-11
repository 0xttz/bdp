from base_fetcher import BaseFetcher
import pandas as pd
import re

class IncomeFetcher(BaseFetcher):
    def __init__(self):
        super().__init__()
        self.table_id = "INDKP107"
        self.exclude_patterns = [
            r'^Region\s',      # Regions
            r'^Landsdel\s',    # Parts of country
            r'^Hele\slandet$', # Whole country
            r'^Udlandet$'      # Foreign
        ]

    def fetch(self):
        """Fetch income data - average disposable income per municipality"""
        try:
            # Years from 2007 onwards
            years = [str(year) for year in range(2007, 2024)]
            
            payload = {
                "table": self.table_id,
                "format": "BULK",
                "variables": [
                    {"code": "OMRÅDE", "values": ["*"]},           # All municipalities
                    {"code": "ENHED", "values": ["116"]},         # Average for all persons (kr.)
                    {"code": "KOEN", "values": ["MOK"]},          # Both genders
                    {"code": "UDDNIV", "values": ["9"]},          # All education levels (Uoplyst = total)
                    {"code": "INDKOMSTTYPE", "values": ["100"]},  # Disposable income
                    {"code": "Tid", "values": years}              # From 2007 onwards
                ]
            }

            self.logger.info(f"Fetching income data")
            df = self.fetch_data(self.table_id, payload)
            
            if not df.empty:
                df = self._clean_dataframe(df)
                self.save_to_csv(df, "municipality_income.csv")
                return df
            return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"Error in income fetch: {str(e)}")
            return pd.DataFrame()

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and format the dataframe"""
        try:
            # Filter out non-municipalities using exclude_patterns
            df = df[~df['OMRÅDE'].str.match('|'.join(self.exclude_patterns))]
            
            # Convert to numeric
            df['INDHOLD'] = pd.to_numeric(df['INDHOLD'], errors='coerce')
            
            # Select only needed columns and rename
            result = df[['OMRÅDE', 'TID', 'INDHOLD']].copy()
            
            # Rename columns
            result = result.rename(columns={
                'OMRÅDE': 'municipality',
                'TID': 'year',
                'INDHOLD': 'avg_income'
            })

            # Sort values
            result = result.sort_values(['municipality', 'year'])
            
            # Add validation
            self._validate_data(result)
            
            return result

        except Exception as e:
            self.logger.error(f"Error cleaning dataframe: {str(e)}")
            self.logger.error(f"DataFrame head:\n{df.head()}")
            return df

    def _validate_data(self, df: pd.DataFrame) -> None:
        """Validate the cleaned data"""
        self.logger.info(f"\nData summary:")
        self.logger.info(f"Year range: {df['year'].min()} - {df['year'].max()}")
        self.logger.info(f"Number of municipalities: {df['municipality'].nunique()}")
        self.logger.info(f"Income range: {df['avg_income'].min():,.0f} - {df['avg_income'].max():,.0f} kr")
        
        # Check for missing values
        missing = df.isnull().sum()
        if missing.any():
            self.logger.warning(f"Missing values found:\n{missing}")

def main():
    fetcher = IncomeFetcher()
    df = fetcher.fetch()
    
if __name__ == "__main__":
    main() 