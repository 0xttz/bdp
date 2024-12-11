from base_fetcher import BaseFetcher
import pandas as pd
import re

class BenefitsFetcher(BaseFetcher):
    def __init__(self):
        super().__init__()
        self.table_id = "AULP01"
        self.exclude_patterns = [
            r'^Region\s',      # Regions
            r'^Landsdel\s',    # Parts of country
            r'^Hele\slandet$', # Whole country
            r'^Udlandet$'      # Foreign
        ]

    def inspect_metadata(self):
        """Inspect table metadata to understand structure"""
        metadata = self.get_table_metadata(self.table_id)
        if metadata:
            self.logger.info("\nAvailable variables:")
            for var in metadata.get('variables', []):
                self.logger.info(f"\nVariable: {var['id']}")
                self.logger.info(f"Description: {var.get('text', 'No description')}")
                self.logger.info("Sample values:")
                for val in var.get('values', [])[:5]:  # Show first 5 values
                    self.logger.info(f"  - {val['id']}: {val.get('text', 'No description')}")
        return metadata

    def is_municipality(self, name: str) -> bool:
        """Check if the area name is a municipality"""
        return not any(re.match(pattern, name) for pattern in self.exclude_patterns)

    def fetch(self):
        """Fetch unemployment percentage data"""
        try:
            # First inspect metadata
            metadata = self.inspect_metadata()
            
            payload = {
                "table": self.table_id,
                "format": "BULK",
                "variables": [
                    {"code": "OMRÅDE", "values": ["*"]},     # All areas
                    {"code": "ALDER", "values": ["TOT"]},    # All age groups
                    {"code": "KØN", "values": ["TOT"]},      # Both genders
                    {"code": "Tid", "values": ["*"]}         # All available periods
                ]
            }

            df = self.fetch_data(self.table_id, payload)
            if not df.empty:
                df = self._clean_dataframe(df)
                self.save_to_csv(df, "municipality_unemployment.csv")
                return df
            return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"Error in benefits fetch: {str(e)}")
            return pd.DataFrame()

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and format the dataframe"""
        try:
            # Filter out non-municipalities
            df = df[df['OMRÅDE'].apply(self.is_municipality)]
            
            # Convert to numeric
            df['INDHOLD'] = pd.to_numeric(df['INDHOLD'].str.replace(',', '.'), errors='coerce')
            
            # Extract year from period (e.g., '2007M01' -> '2007')
            df['year'] = df['TID'].str[:4]
            
            # Calculate yearly average for each municipality
            df_yearly = df.groupby(['OMRÅDE', 'year'])['INDHOLD'].mean().reset_index()
            
            # Rename columns
            df_yearly = df_yearly.rename(columns={
                'OMRÅDE': 'municipality',
                'INDHOLD': 'unemployment_pct'
            })

            # Select and order columns
            df_yearly = df_yearly[['municipality', 'year', 'unemployment_pct']]
            
            # Sort values
            df_yearly = df_yearly.sort_values(['municipality', 'year'])
            
            # Log some statistics for validation
            self.logger.info(f"\nData summary:")
            self.logger.info(f"Year range: {df_yearly['year'].min()} - {df_yearly['year'].max()}")
            self.logger.info(f"Number of municipalities: {df_yearly['municipality'].nunique()}")
            self.logger.info(f"Unemployment range: {df_yearly['unemployment_pct'].min():.1f}% - {df_yearly['unemployment_pct'].max():.1f}%")
            
            return df_yearly

        except Exception as e:
            self.logger.error(f"Error cleaning dataframe: {str(e)}")
            return df

def main():
    fetcher = BenefitsFetcher()
    # First run to inspect metadata
    df = fetcher.fetch()
    
if __name__ == "__main__":
    main()