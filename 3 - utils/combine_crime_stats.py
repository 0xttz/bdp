import pandas as pd
import numpy as np
from pathlib import Path

def load_csv_with_encoding(filepath):
    """Try loading CSV with different encodings"""
    encodings = ['utf-8', 'iso-8859-1', 'cp1252', 'latin1']
    
    for encoding in encodings:
        try:
            return pd.read_csv(filepath, encoding=encoding)
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"Error reading file with {encoding} encoding: {str(e)}")
            continue
    
    raise ValueError(f"Could not read {filepath} with any of the attempted encodings")

def extract_year_quarter(quarter_str):
    """Extract year and quarter number from strings like '2007Q1'"""
    try:
        year = quarter_str[:4]
        quarter = quarter_str[-1]
        return pd.Series({'year': int(year), 'quarter_num': int(quarter)})
    except Exception as e:
        print(f"Error parsing quarter string '{quarter_str}': {str(e)}")
        return pd.Series({'year': np.nan, 'quarter_num': np.nan})

def process_regionwise_data(df):
    """Process the regionwise crime data"""
    melted = pd.melt(
        df,
        id_vars=['REGION', 'TYPE OF OFFENCE'],
        var_name='quarter',
        value_name='count'
    )
    
    melted.columns = ['region', 'offense_type', 'quarter', 'count']
    
    year_quarter = melted['quarter'].apply(extract_year_quarter)
    melted['year'] = year_quarter['year']
    melted['quarter_num'] = year_quarter['quarter_num']
    
    melted['source'] = 'regionwise'
    
    return melted

def process_statistics_data(df):
    """Process the general statistics data"""
    processed_df = df.copy()    
    processed_df.columns = processed_df.columns.str.lower().str.replace(' ', '_')
    processed_df['source'] = 'statistics'
    
    return processed_df

def combine_datasets():
    """Combine both crime statistics datasets"""
    try:
        regionwise_path = Path('Denmark_Crime_Regionwise.csv')
        statistics_path = Path('denmark_crime_statistics.csv')
        
        if not regionwise_path.exists():
            raise FileNotFoundError(f"File not found: {regionwise_path}")
        if not statistics_path.exists():
            raise FileNotFoundError(f"File not found: {statistics_path}")
        
        print("Loading and processing regionwise data...")
        regionwise_df = load_csv_with_encoding(regionwise_path)
        regionwise_processed = process_regionwise_data(regionwise_df)
        
        print("Loading and processing statistics data...")
        statistics_df = load_csv_with_encoding(statistics_path)
        statistics_processed = process_statistics_data(statistics_df)
        
        print("Combining datasets...")
        output_path_regionwise = Path('processed_regionwise_crime.csv')
        output_path_statistics = Path('processed_statistics_crime.csv')
        
        regionwise_processed.to_csv(output_path_regionwise, index=False, encoding='utf-8')
        statistics_processed.to_csv(output_path_statistics, index=False, encoding='utf-8')

        print(f"Processing complete. Files saved:")
        print(f"- {output_path_regionwise}")
        print(f"- {output_path_statistics}")
        
        return regionwise_processed, statistics_processed
        
    except Exception as e:
        print(f"Error processing data: {str(e)}")
        return None, None

def validate_data(regionwise_df, statistics_df):
    """Validate the processed data"""
    try:
        validation_results = {
            'regionwise_data': {
                'missing_values': regionwise_df.isnull().sum().to_dict(),
                'negative_counts': (regionwise_df['count'] < 0).sum(),
                'duplicate_records': regionwise_df.duplicated().sum(),
                'total_rows': len(regionwise_df),
                'memory_usage': f"{regionwise_df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB",
                'year_range': f"{regionwise_df['year'].min()} - {regionwise_df['year'].max()}",
                'unique_regions': regionwise_df['region'].nunique(),
                'unique_offense_types': regionwise_df['offense_type'].nunique()
            },
            'statistics_data': {
                'missing_values': statistics_df.isnull().sum().to_dict(),
                'total_rows': len(statistics_df),
                'memory_usage': f"{statistics_df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB"
            }
        }
        return validation_results
    except Exception as e:
        print(f"Error during validation: {str(e)}")
        return None

if __name__ == "__main__":
    print("Starting data processing...")
    
    regionwise_data, statistics_data = combine_datasets()

    if regionwise_data is not None and statistics_data is not None:
        print("\nPerforming data validation...")
        validation = validate_data(regionwise_data, statistics_data)
        
        if validation:
            print("\nData Validation Results:")
            print("\nRegionwise Data:")
            for key, value in validation['regionwise_data'].items():
                print(f"{key}:", value)
            
            print("\nStatistics Data:")
            for key, value in validation['statistics_data'].items():
                print(f"{key}:", value)
            
            print("\nFirst few rows of regionwise data:")
            print(regionwise_data.head())
            
            print("\nFirst few rows of statistics data:")
            print(statistics_data.head())
    else:
        print("Data processing failed. Please check the error messages above.")