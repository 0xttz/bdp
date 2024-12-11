import pandas as pd
import numpy as np
from pathlib import Path

def load_processed_data():
    """Load the processed CSV files"""
    try:
        regionwise_df = pd.read_csv('processed_regionwise_crime.csv')
        statistics_df = pd.read_csv('processed_statistics_crime.csv')
        return regionwise_df, statistics_df
    except Exception as e:
        print(f"Error loading processed files: {str(e)}")
        return None, None

def standardize_statistics_df(statistics_df):
    """Standardize the statistics dataframe to match regionwise format"""
    column_mapping = {
        'område': 'region',
        'overtræd': 'offense_type',
        'tid': 'quarter',
        'indhold': 'count'
    }
    
    stats_df = statistics_df.rename(columns=column_mapping)
    
    stats_df['year'] = stats_df['quarter'].str[:4].astype(int)
    stats_df['quarter_num'] = stats_df['quarter'].str[-1].astype(int)
    
    return stats_df

def merge_datasets(regionwise_df, statistics_df):
    """Merge the two datasets based on common fields"""
    try:
        reg_df = regionwise_df.copy()
        stat_df = standardize_statistics_df(statistics_df)
        
        reg_df['date'] = pd.to_datetime(
            reg_df['year'].astype(str) + 'Q' + 
            reg_df['quarter_num'].astype(str)
        )
        
        stat_df['date'] = pd.to_datetime(
            stat_df['year'].astype(str) + 'Q' + 
            stat_df['quarter_num'].astype(str)
        )
        
        common_columns = ['region', 'offense_type', 'quarter', 'count', 'year', 
                         'quarter_num', 'source', 'date']
        
        reg_df = reg_df[common_columns]
        stat_df = stat_df[common_columns]
        
        merged_df = pd.concat([reg_df, stat_df], ignore_index=True)
        
        merged_df = merged_df.sort_values(['region', 'offense_type', 'date'])
        
        merged_df['processed_date'] = pd.Timestamp.now()
        
        return merged_df
        
    except Exception as e:
        print(f"Error merging datasets: {str(e)}")
        return None

def validate_merged_data(merged_df):
    """Validate the merged dataset"""
    validation = {
        'total_records': len(merged_df),
        'records_by_source': merged_df['source'].value_counts().to_dict(),
        'missing_values': merged_df.isnull().sum().to_dict(),
        'date_range': f"{merged_df['date'].min()} to {merged_df['date'].max()}",
        'regions': merged_df['region'].nunique(),
        'offense_types': merged_df['offense_type'].nunique(),
        'memory_usage': f"{merged_df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB"
    }
    return validation

def save_merged_data(merged_df):
    """Save the merged dataset"""
    try:
        output_path = Path('final_crime_statistics.csv')
        merged_df.to_csv(output_path, index=False)
        print(f"Merged data saved to {output_path}")
        return True
    except Exception as e:
        print(f"Error saving merged data: {str(e)}")
        return False

def main():
    print("Starting merge process...")
    
    regionwise_df, statistics_df = load_processed_data()
    if regionwise_df is None or statistics_df is None:
        return
    
    print("\nData loaded successfully")
    print(f"Regionwise shape: {regionwise_df.shape}")
    print(f"Statistics shape: {statistics_df.shape}")
    print("\nMerging datasets...")
    merged_df = merge_datasets(regionwise_df, statistics_df)
    
    if merged_df is not None:
        print("\nValidating merged data...")
        validation = validate_merged_data(merged_df)
        
        print("\nValidation Results:")
        for key, value in validation.items():
            print(f"{key}:", value)
        
        print("\nSaving merged data...")
        if save_merged_data(merged_df):
            print("\nMerge process completed successfully")
            
            print("\nSample of regionwise data:")
            print(merged_df[merged_df['source'] == 'regionwise'].head())
            
            print("\nSample of statistics data:")
            print(merged_df[merged_df['source'] == 'statistics'].head())
            
            print("\nShape of merged data:", merged_df.shape)
    else:
        print("Merge process failed")

if __name__ == "__main__":
    main() 