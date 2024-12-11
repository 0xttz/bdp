import pandas as pd
import numpy as np
from pathlib import Path

def load_population_data(filepath: str) -> pd.DataFrame:
    """Load and process population data"""
    df = pd.read_csv(filepath)
    return df[['municipality', 'year', 'population']].rename(columns={'population': 'total_population'})

def load_income_data(filepath: str) -> pd.DataFrame:
    """Load and process income data"""
    df = pd.read_csv(filepath)
    df['avg_income'] = df.groupby('municipality')['avg_income'].fillna(method='ffill')
    return df[['municipality', 'year', 'avg_income']]

def load_inequality_data(filepath: str) -> pd.DataFrame:
    """Load and process inequality (Gini) data"""
    df = pd.read_csv(filepath)
    df['gini_coefficient'] = df.groupby('municipality')['gini_coefficient'].fillna(method='ffill')
    mean = df['gini_coefficient'].mean()
    std = df['gini_coefficient'].std()
    df.loc[df['gini_coefficient'] > mean + 3*std, 'gini_coefficient'] = np.nan
    return df[['municipality', 'year', 'gini_coefficient']]

def load_unemployment_data(filepath: str) -> pd.DataFrame:
    """Load and process unemployment data"""
    df = pd.read_csv(filepath)
    df = df.rename(columns={
        'OMRÅDE': 'municipality',
        'TID': 'year',
        'INDHOLD': 'unemployment_rate'
    })
    df['unemployment_rate'] = df.groupby('municipality')['unemployment_rate'].fillna(method='ffill')
    return df[['municipality', 'year', 'unemployment_rate']]

def load_education_percentages(filepath: str) -> pd.DataFrame:
    """Load pre-calculated education percentages"""
    df = pd.read_csv(filepath)
    df = df[df['education_level'] == 'I alt']
    return df[['municipality', 'year', 'percentage']].rename(
        columns={'percentage': 'share_of_higher_educated'})

def merge_datasets(base_dir: Path) -> pd.DataFrame:
    """Merge all datasets into one consistent table"""
    population_path = base_dir / "municipality_population.csv"
    income_path = base_dir / "municipality_income.csv"
    inequality_path = base_dir / "municipality_inequality.csv"
    unemployment_path = base_dir / "municipality_unemployment.csv"
    education_path = base_dir / "municipality_education_percentages.csv"
    
    print("Loading datasets...")
    population_df = load_population_data(population_path)
    income_df = load_income_data(income_path)
    inequality_df = load_inequality_data(inequality_path)
    unemployment_df = load_unemployment_data(unemployment_path)
    education_df = load_education_percentages(education_path)
    
    print("Merging datasets...")
    merged_df = population_df.copy()
    
    for df, name in [
        (income_df, 'income'),
        (inequality_df, 'inequality'),
        (unemployment_df, 'unemployment'),
        (education_df, 'education')
    ]:
        merged_df = pd.merge(
            merged_df, df,
            on=['municipality', 'year'],
            how='left',
            validate='1:1',
            indicator=True
        )
        print(f"{name.title()} merge status:")
        print(merged_df['_merge'].value_counts())
        merged_df.drop('_merge', axis=1, inplace=True)
    

    for col in ['avg_income', 'gini_coefficient', 'unemployment_rate', 'share_of_higher_educated']:
        merged_df[col] = merged_df.groupby('municipality')[col].fillna(method='ffill')
        merged_df[col] = merged_df.groupby('municipality')[col].fillna(method='bfill')
    
    merged_df['total_population'] = merged_df['total_population'].round(0)
    merged_df['avg_income'] = merged_df['avg_income'].round(0)
    merged_df['gini_coefficient'] = merged_df['gini_coefficient'].round(2)
    merged_df['unemployment_rate'] = merged_df['unemployment_rate'].round(1)
    merged_df['share_of_higher_educated'] = merged_df['share_of_higher_educated'].round(1)
    
    return merged_df.sort_values(['municipality', 'year'])

def check_data_quality(df: pd.DataFrame) -> dict:
    """Perform detailed data quality checks"""
    return {
        'total_rows': len(df),
        'municipalities': df['municipality'].nunique(),
        'year_range': f"{df['year'].min()}-{df['year'].max()}",
        'missing_values': df.isnull().sum().to_dict(),
        'data_ranges': {col: f"{df[col].min():.1f}-{df[col].max():.1f}"
                       for col in df.select_dtypes(include=[np.number]).columns},
        'municipalities_with_gaps': {
            col: df[df[col].isnull()]['municipality'].unique().tolist()
            for col in df.columns if df[col].isnull().any()
        }
    }

def main():
    base_dir = Path.cwd().parent / "socio" / "mining_scripts" / "csvs-to-process"
    output_path = base_dir / "municipality_socio_economic.csv"
    
    print(f"Processing files from: {base_dir}")
    
    try:
        merged_df = merge_datasets(base_dir)
        quality_results = check_data_quality(merged_df)
        
        merged_df.to_csv(output_path, index=False)
        
        print("\nData Quality Report:")
        for key, value in quality_results.items():
            print(f"\n{key}:")
            if isinstance(value, dict):
                for k, v in value.items():
                    print(f"  {k}: {v}")
            else:
                print(f"  {value}")
        
        print("\nSample of final data:")
        print(merged_df.head())
        
    except Exception as e:
        print(f"Error processing data: {str(e)}")

if __name__ == "__main__":
    main()