from base_fetcher import BaseFetcher
import pandas as pd

class InequalityFetcher(BaseFetcher):
    def __init__(self):
        super().__init__()
        self.table_id = "IFOR41"

    def fetch(self):
        """Fetch income inequality (Gini) data from 2007 onwards"""
        try:
            # Years from 2007 onwards
            years = [str(year) for year in range(2007, 2024)]
            
            payload = {
                "table": self.table_id,
                "format": "BULK",
                "variables": [
                    {"code": "ULLIG", "values": ["70"]},     # Gini coefficient
                    {"code": "KOMMUNEDK", "values": ["*"]},  # All municipalities
                    {"code": "Tid", "values": years}         # From 2007 onwards
                ]
            }

            df = self.fetch_data(self.table_id, payload)
            if not df.empty:
                df = self._clean_dataframe(df)
                self.save_to_csv(df, "municipality_inequality.csv")
                return df
            return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"Error in inequality fetch: {str(e)}")
            return pd.DataFrame()

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and format the dataframe"""
        try:
            # Convert to numeric
            df['INDHOLD'] = pd.to_numeric(df['INDHOLD'].str.replace(',', '.'), errors='coerce')
            
            # Rename columns
            df = df.rename(columns={
                'KOMMUNEDK': 'municipality',
                'TID': 'year',
                'INDHOLD': 'gini_coefficient'
            })

            # Select and order columns
            df = df[['municipality', 'year', 'gini_coefficient']]
            
            # Sort values
            df = df.sort_values(['municipality', 'year'])
            
            return df

        except Exception as e:
            self.logger.error(f"Error cleaning dataframe: {str(e)}")
            return df

    def _validate_data(self, df: pd.DataFrame) -> None:
        """Validate the cleaned data"""
        self.logger.info(f"\nData summary:")
        self.logger.info(f"Year range: {df['year'].min()} - {df['year'].max()}")
        self.logger.info(f"Number of municipalities: {df['municipality'].nunique()}")
        self.logger.info(f"Gini range: {df['gini_coefficient'].min():.2f} - {df['gini_coefficient'].max():.2f}")
        
        # Check for outliers (>3 std from mean)
        mean = df['gini_coefficient'].mean()
        std = df['gini_coefficient'].std()
        outliers = df[abs(df['gini_coefficient'] - mean) > 3*std]
        
        if not outliers.empty:
            self.logger.warning(f"Found outliers:\n{outliers}")

def main():
    fetcher = InequalityFetcher()
    df = fetcher.fetch()
    
if __name__ == "__main__":
    main() 