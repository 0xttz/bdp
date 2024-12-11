from base_fetcher import BaseFetcher
import pandas as pd
import re

class EducationFetcher(BaseFetcher):
    def __init__(self):
        super().__init__()
        self.table_id = "LIGEUB1"
        self.exclude_patterns = [
            r'^Region\s',      
            r'^Landsdel\s',    
            r'^Hele\slandet$', 
            r'^Udlandet$'      
        ]
        self.higher_ed_codes = [
            "H40",  # KVU (Short cycle)
            "H50",  # MVU (Medium cycle)
            "H60",  # BACH (Bachelor)
            "H70",  # LVU (Long cycle)
            "H80"   # Ph.d.
        ]

    def is_municipality(self, name: str) -> bool:
        """Check if the area name is a municipality"""
        return not any(re.match(pattern, name) for pattern in self.exclude_patterns)

    def fetch(self):
        """Fetch education data - percentage with higher education"""
        try:
            years = [str(year) for year in range(2007, 2024)]
            
            payload = {
                "table": self.table_id,
                "format": "BULK",
                "variables": [
                    {"code": "BOPOMR", "values": ["*"]},           
                    {"code": "HERKOMST", "values": ["00"]},     
                    {"code": "HFUDD", "values": ["I alt"]},     
                    {"code": "ALDER", "values": ["TOT"]},       
                    {"code": "KØN", "values": ["TOT"]},         
                    {"code": "Tid", "values": years}           
                ]
            }

            self.logger.info(f"Fetching total population data")
            total_df = self.fetch_data(self.table_id, payload)

            payload["variables"][2] = {"code": "HFUDD", "values": self.higher_ed_codes}  
            self.logger.info(f"Fetching higher education data")
            higher_ed_df = self.fetch_data(self.table_id, payload)

            if not total_df.empty and not higher_ed_df.empty:
                df = pd.concat([total_df, higher_ed_df])
                df = self._clean_dataframe(df)
                self.save_to_csv(df, "municipality_education.csv")
                return df
            return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"Error in education fetch: {str(e)}")
            return pd.DataFrame()

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and format the dataframe"""
        try:
            df = df[df['BOPOMR'].apply(self.is_municipality)]
            
            df['INDHOLD'] = pd.to_numeric(df['INDHOLD'], errors='coerce')
            
            total_df = df[df['HFUDD'] == 'I alt'].copy()  
            higher_ed_df = df[df['HFUDD'].str.startswith(tuple(self.higher_ed_codes))].copy()
            
            self.logger.info(f"Total records: {len(df)}")
            self.logger.info(f"Total population records: {len(total_df)}")
            self.logger.info(f"Higher education records: {len(higher_ed_df)}")
            
            if total_df.empty or higher_ed_df.empty:
                self.logger.error("Missing data - total or higher education dataframe is empty")
                self.logger.error(f"Unique HFUDD values: {df['HFUDD'].unique()}")
                return df
            
            higher_ed_sum = higher_ed_df.groupby(['BOPOMR', 'TID'])['INDHOLD'].sum().reset_index()
            total_counts = total_df.groupby(['BOPOMR', 'TID'])['INDHOLD'].first().reset_index()
            
            result = pd.merge(higher_ed_sum, total_counts, on=['BOPOMR', 'TID'], suffixes=('_higher', '_total'))
            result['higher_education_pct'] = (result['INDHOLD_higher'] / result['INDHOLD_total']) * 100
            
            result = result.rename(columns={
                'BOPOMR': 'municipality',
                'TID': 'year'
            })[['municipality', 'year', 'higher_education_pct']]
            
            result = result.sort_values(['municipality', 'year'])
            
            self._validate_data(result)
            
            return result

        except Exception as e:
            self.logger.error(f"Error cleaning dataframe: {str(e)}")
            self.logger.error(f"DataFrame head:\n{df.head()}")
            self.logger.error(f"HFUDD unique values:\n{df['HFUDD'].unique()}")
            return df

    def _validate_data(self, df: pd.DataFrame) -> None:
        """Validate the cleaned data"""
        self.logger.info(f"\nData summary:")
        self.logger.info(f"Year range: {df['year'].min()} - {df['year'].max()}")
        self.logger.info(f"Number of municipalities: {df['municipality'].nunique()}")
        self.logger.info(f"Education range: {df['higher_education_pct'].min():.1f}% - {df['higher_education_pct'].max():.1f}%")
        
        missing = df.isnull().sum()
        if missing.any():
            self.logger.warning(f"Missing values found:\n{missing}")
        
            if (df['higher_education_pct'] > 100).any():
                self.logger.error("Found percentages greater than 100%")
            if (df['higher_education_pct'] < 0).any():
                self.logger.error("Found negative percentages")

def main():
    fetcher = EducationFetcher()
    df = fetcher.fetch()
    
if __name__ == "__main__":
    main() 