from base_fetcher import BaseFetcher
import pandas as pd

class PopulationFetcher(BaseFetcher):
    def __init__(self):
        super().__init__()
        self.table_id = "FOLK1A"
        self.exclude_patterns = [
            r'^Region\s',      # Regions
            r'^Landsdel\s',    # Parts of country
            r'^Hele\slandet$', # Whole country
            r'^Udlandet$'      # Foreign
        ]

    def fetch(self):
        """Fetch population data from 2008 onwards, quarterly data aggregated to yearly"""
        try:
            # Generate quarters from 2008K1 to 2023K4
            quarters = []
            for year in range(2008, 2024):
                for quarter in range(1, 5):
                    quarters.append(f"{year}K{quarter}")
            
            payload = {
                "table": self.table_id,
                "format": "BULK",
                "variables": [
                    {"code": "OMRÅDE", "values": ["*"]},           # All municipalities
                    {"code": "ALDER", "values": ["IALT"]},        # Total population
                    {"code": "KØN", "values": ["TOT"]},           # Both genders
                    {"code": "CIVILSTAND", "values": ["TOT"]},    # All marital statuses
                    {"code": "Tid", "values": quarters}           # Quarterly data
                ]
            }

            self.logger.info(f"Fetching quarterly population data")
            df = self.fetch_data(self.table_id, payload)
            
            if not df.empty:
                df = self._clean_dataframe(df)
                self.save_to_csv(df, "municipality_population.csv")
                return df
            return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"Error in population fetch: {str(e)}")
            return pd.DataFrame()

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and format the dataframe"""
        try:
            # Filter out non-municipalities
            df = df[~df['OMRÅDE'].str.match('|'.join(self.exclude_patterns))]
            
            # Convert to numeric
            df['INDHOLD'] = pd.to_numeric(df['INDHOLD'], errors='coerce')
            
            # Extract year from quarter (e.g., '2008K1' -> '2008')
            df['year'] = df['TID'].str[:4]
            
            # Group by municipality and year to get yearly average
            result = df.groupby(['OMRÅDE', 'year'])['INDHOLD'].mean().reset_index()
            
            # Rename columns
            result = result.rename(columns={
                'OMRÅDE': 'municipality',
                'INDHOLD': 'population'
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
        self.logger.info(f"Population range: {df['population'].min():,.0f} - {df['population'].max():,.0f}")
        
        # Check for missing values
        missing = df.isnull().sum()
        if missing.any():
            self.logger.warning(f"Missing values found:\n{missing}")

def main():
    fetcher = PopulationFetcher()
    df = fetcher.fetch()
    
if __name__ == "__main__":
    main() 