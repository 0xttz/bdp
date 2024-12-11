import pandas as pd
import numpy as np
from pathlib import Path

REGION_MAPPING = {
    # Major Cities (separated)
    'København': 'Copenhagen',  # Danish spelling
    'Aarhus': 'Aarhus',
    'Odense': 'Odense',
    'Aalborg': 'Aalborg',
    
    # Region Hovedstaden (Capital Region) - Copenhagen removed
    'Frederiksberg': 'Hovedstaden',
    'Albertslund': 'Hovedstaden',
    'Allerød': 'Hovedstaden',
    'Ballerup': 'Hovedstaden',
    'Bornholm': 'Hovedstaden',
    'Brøndby': 'Hovedstaden',
    'Dragør': 'Hovedstaden',
    'Egedal': 'Hovedstaden',
    'Fredensborg': 'Hovedstaden',
    'Frederikssund': 'Hovedstaden',
    'Furesø': 'Hovedstaden',
    'Gentofte': 'Hovedstaden',
    'Gladsaxe': 'Hovedstaden',
    'Glostrup': 'Hovedstaden',
    'Gribskov': 'Hovedstaden',
    'Halsnæs': 'Hovedstaden',
    'Helsingør': 'Hovedstaden',
    'Herlev': 'Hovedstaden',
    'Hillerød': 'Hovedstaden',
    'Hvidovre': 'Hovedstaden',
    'Høje-Taastrup': 'Hovedstaden',
    'Hørsholm': 'Hovedstaden',
    'Ishøj': 'Hovedstaden',
    'Lyngby-Taarbæk': 'Hovedstaden',
    'Rudersdal': 'Hovedstaden',
    'Rødovre': 'Hovedstaden',
    'Tårnby': 'Hovedstaden',
    'Vallensbæk': 'Hovedstaden',
}

# Add the rest of the regions from transform_crime_data.py
REGION_MAPPING.update({
    # Add Region Sjælland municipalities
    'Faxe': 'Sjælland',
    'Greve': 'Sjælland',
    'Guldborgsund': 'Sjælland',
    'Holbæk': 'Sjælland',
    'Kalundborg': 'Sjælland',
    'Køge': 'Sjælland',
    'Lejre': 'Sjælland',
    'Lolland': 'Sjælland',
    'Næstved': 'Sjælland',
    'Odsherred': 'Sjælland',
    'Ringsted': 'Sjælland',
    'Roskilde': 'Sjælland',
    'Slagelse': 'Sjælland',
    'Solrød': 'Sjælland',
    'Sorø': 'Sjælland',
    'Stevns': 'Sjælland',
    'Vordingborg': 'Sjælland',
    
    # Add remaining regions from the crime data mapping...
    # [Rest of the mapping from transform_crime_data.py]
})

def load_population_data(filepath: str) -> pd.DataFrame:
    """Load and process population data"""
    df = pd.read_csv(filepath)
    print("\nPopulation data analysis:")
    print(f"Year range: {df['year'].min()}-{df['year'].max()}")
    print(f"Number of municipalities: {df['municipality'].nunique()}")
    return df[['municipality', 'year', 'population']].rename(columns={'population': 'total_population'})

def load_income_data(filepath: str) -> pd.DataFrame:
    """Load and process income data"""
    df = pd.read_csv(filepath)
    
    # Check for missing values before processing
    print("\nIncome data analysis:")
    print(f"Missing values before processing: {df['avg_income'].isnull().sum()}")
    print("Years with missing data:", 
          df[df['avg_income'].isnull()]['year'].unique())
    print("Municipalities with missing data:", 
          df[df['avg_income'].isnull()]['municipality'].unique())
    
    # Use interpolation for missing values
    df['avg_income'] = df.groupby('municipality')['avg_income'].transform(
        lambda x: x.interpolate(method='linear', limit_direction='both'))
    
    return df[['municipality', 'year', 'avg_income']]

def load_inequality_data(filepath: str) -> pd.DataFrame:
    """Load and process inequality (Gini) data"""
    df = pd.read_csv(filepath)
    
    # Check for missing values before processing
    print("\nInequality data analysis:")
    print(f"Missing values before processing: {df['gini_coefficient'].isnull().sum()}")
    print("Years with missing data:", 
          df[df['gini_coefficient'].isnull()]['year'].unique())
    print("Municipalities with missing data:", 
          df[df['gini_coefficient'].isnull()]['municipality'].unique())
    
    # Use interpolation for missing values
    df['gini_coefficient'] = df.groupby('municipality')['gini_coefficient'].transform(
        lambda x: x.interpolate(method='linear', limit_direction='both'))
    
    # Remove only 'Hele landet' instead of outliers
    df = df[df['municipality'] != 'Hele landet']
    
    return df[['municipality', 'year', 'gini_coefficient']]

def load_unemployment_data(filepath: str) -> pd.DataFrame:
    """Load and process unemployment data"""
    df = pd.read_csv(filepath)
    
    # Check for missing values before processing
    print("\nUnemployment data analysis:")
    print(f"Missing values before processing: {df['INDHOLD'].isnull().sum()}")
    print("Years with missing data:", 
          df[df['INDHOLD'].isnull()]['TID'].unique())
    print("Municipalities with missing data:", 
          df[df['INDHOLD'].isnull()]['OMRÅDE'].unique())
    
    df = df.rename(columns={
        'OMRÅDE': 'municipality',
        'TID': 'year',
        'INDHOLD': 'unemployment_rate'
    })
    
    # Use interpolation for missing values
    df['unemployment_rate'] = df.groupby('municipality')['unemployment_rate'].transform(
        lambda x: x.interpolate(method='linear', limit_direction='both'))
    
    return df[['municipality', 'year', 'unemployment_rate']]

def validate_municipality_names(dfs: dict) -> list:
    """Check for inconsistencies in municipality names across datasets"""
    municipalities = {}
    inconsistencies = []
    
    for name, df in dfs.items():
        muni_set = set(df['municipality'].unique())
        municipalities[name] = muni_set
        
        for other_name, other_set in municipalities.items():
            if name != other_name:
                diff = muni_set.symmetric_difference(other_set)
                if diff:
                    inconsistencies.append(f"Difference between {name} and {other_name}: {diff}")
    
    return inconsistencies

def analyze_data_coverage(dfs: dict):
    """Analyze data coverage and inconsistencies across all datasets"""
    print("\nData Coverage Analysis:")
    
    # Check year coverage
    print("\nYear coverage by dataset:")
    for name, df in dfs.items():
        years = sorted(df['year'].unique())
        print(f"{name}: {min(years)}-{max(years)} ({len(years)} years)")
        
    # Check municipality coverage
    print("\nMunicipality coverage by dataset:")
    for name, df in dfs.items():
        print(f"{name}: {df['municipality'].nunique()} municipalities")
        
    # Check for missing values
    print("\nMissing values by dataset:")
    for name, df in dfs.items():
        missing = df.isnull().sum()
        if missing.any():
            print(f"\n{name}:")
            print(missing[missing > 0])

def load_education_percentages(filepath: str) -> pd.DataFrame:
    """Load pre-calculated education percentages"""
    try:
        df = pd.read_csv(filepath)
        
        print("\nEducation data analysis:")
        print(f"Total records: {len(df)}")
        print(f"Unique municipalities: {df['municipality'].nunique()}")
        print(f"Year range: {df['year'].min()}-{df['year'].max()}")
        
        # Filter for total education percentage
        df = df[df['education_level'] == 'I alt']
        df = df[['municipality', 'year', 'percentage']].rename(
            columns={'percentage': 'share_of_higher_educated'})
        
        # Handle any missing values
        df['share_of_higher_educated'] = df.groupby('municipality')['share_of_higher_educated'].transform(
            lambda x: x.interpolate(method='linear', limit_direction='both'))
        
        return df
    except Exception as e:
        print(f"Error loading education data: {str(e)}")
        return pd.DataFrame()

def merge_datasets(base_dir: Path) -> pd.DataFrame:
    """Merge all datasets into one consistent table"""
    # Define file paths
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
    
    # Remove nationwide data and Christiansø
    municipalities_to_remove = ['Hele landet', 'Christiansø']
    for df in [population_df, income_df, inequality_df, unemployment_df, education_df]:
        if not df.empty and 'municipality' in df.columns:
            df.drop(df[df['municipality'].isin(municipalities_to_remove)].index, inplace=True)
    
    # Start with population data as base
    print("\nMerging datasets...")
    merged_df = population_df.copy()
    
    # Merge with other datasets and investigate mismatches
    for df, name in [
        (income_df, 'income'),
        (inequality_df, 'inequality'),
        (unemployment_df, 'unemployment'),
        (education_df, 'education')
    ]:
        # Before merge, print detailed info
        if name == 'income':
            print("\nInvestigating income merge mismatches:")
            print("\nPopulation data unique municipality-years:")
            pop_keys = set(zip(population_df['municipality'], population_df['year']))
            print(f"Total: {len(pop_keys)}")
            
            print("\nIncome data unique municipality-years:")
            income_keys = set(zip(df['municipality'], df['year']))
            print(f"Total: {len(income_keys)}")
            
            # Find mismatches
            missing_in_income = pop_keys - income_keys
            extra_in_income = income_keys - pop_keys
            
            if missing_in_income:
                print("\nMissing in income data:")
                for muni, year in sorted(missing_in_income):
                    print(f"Municipality: {muni}, Year: {year}")
            
            if extra_in_income:
                print("\nExtra in income data:")
                for muni, year in sorted(extra_in_income):
                    print(f"Municipality: {muni}, Year: {year}")
        
        merged_df = pd.merge(
            merged_df, df,
            on=['municipality', 'year'],
            how='left',
            validate='1:1',
            indicator=True
        )
        print(f"\n{name.title()} merge status:")
        print(merged_df['_merge'].value_counts())
        merged_df.drop('_merge', axis=1, inplace=True)
    
    # Post-processing
    for col in ['avg_income', 'gini_coefficient', 'unemployment_rate', 'share_of_higher_educated']:
        merged_df[col] = merged_df.groupby('municipality')[col].transform(
            lambda x: x.interpolate(method='linear', limit_direction='both'))
    
    # Round numeric columns
    merged_df['total_population'] = merged_df['total_population'].round(0)
    merged_df['avg_income'] = merged_df['avg_income'].round(0)
    merged_df['gini_coefficient'] = merged_df['gini_coefficient'].round(2)
    merged_df['unemployment_rate'] = merged_df['unemployment_rate'].round(1)
    merged_df['share_of_higher_educated'] = merged_df['share_of_higher_educated'].round(1)
    
    print(f"\nFinal number of municipalities: {merged_df['municipality'].nunique()}")
    
    # Add broader region mapping
    merged_df['broader_region'] = merged_df['municipality'].map(REGION_MAPPING)
    
    # Verify all municipalities are mapped
    unmapped = merged_df[merged_df['broader_region'].isna()]['municipality'].unique()
    if len(unmapped) > 0:
        print("\nWarning: Some municipalities are not mapped to broader regions:")
        print(unmapped)
    
    # Reorder columns to put region first
    column_order = [
        'broader_region',
        'municipality',
        'year',
        'total_population',
        'avg_income',
        'gini_coefficient',
        'unemployment_rate',
        'share_of_higher_educated'
    ]
    merged_df = merged_df[column_order]
    
    # Sort by region, municipality, and year
    merged_df = merged_df.sort_values(['broader_region', 'municipality', 'year'])
    
    print("\nBroader Region Statistics:")
    region_stats = merged_df.groupby('broader_region').agg({
        'municipality': 'nunique',
        'total_population': 'mean',
        'avg_income': 'mean',
        'gini_coefficient': 'mean',
        'unemployment_rate': 'mean',
        'share_of_higher_educated': 'mean'
    }).round(2)
    print(region_stats)
    
    return merged_df

def validate_data(df: pd.DataFrame) -> dict:
    """Validate the merged dataset"""
    return {
        'total_rows': len(df),
        'municipalities': df['municipality'].nunique(),
        'year_range': f"{df['year'].min()}-{df['year'].max()}",
        'missing_values': df.isnull().sum().to_dict(),
        'data_ranges': {
            'total_population': f"{df['total_population'].min():.0f}-{df['total_population'].max():.0f}",
            'avg_income': f"{df['avg_income'].min():.0f}-{df['avg_income'].max():.0f}",
            'gini_coefficient': f"{df['gini_coefficient'].min():.2f}-{df['gini_coefficient'].max():.2f}",
            'unemployment_rate': f"{df['unemployment_rate'].min():.2f}-{df['unemployment_rate'].max():.2f}",
            'share_of_higher_educated': f"{df['share_of_higher_educated'].min():.2f}-{df['share_of_higher_educated'].max():.2f}"
        }
    }

def main():
    base_dir = Path.cwd() / "mining_scripts" / "csvs-to-process"
    output_path = base_dir / "municipality_socio_economic.csv"
    
    print(f"Processing files from: {base_dir}")
    
    try:
        # Load all datasets
        datasets = {
            'population': load_population_data(base_dir / "municipality_population.csv"),
            'income': load_income_data(base_dir / "municipality_income.csv"),
            'inequality': load_inequality_data(base_dir / "municipality_inequality.csv"),
            'unemployment': load_unemployment_data(base_dir / "municipality_unemployment.csv")
        }
        
        # Check for municipality name inconsistencies
        inconsistencies = validate_municipality_names(datasets)
        if inconsistencies:
            print("\nMunicipality name inconsistencies found:")
            for inc in inconsistencies:
                print(inc)
        
        # Analyze data coverage
        analyze_data_coverage(datasets)
        
        # Merge datasets
        print("\nMerging datasets...")
        merged_df = merge_datasets(base_dir)
        
        # Validate the merged data
        validation_results = validate_data(merged_df)
        
        # Save the merged dataset
        merged_df.to_csv(output_path, index=False)
        
        print("\nData processing completed successfully!")
        print(f"Output saved to: {output_path}")
        print("\nValidation Results:")
        for key, value in validation_results.items():
            print(f"{key}:")
            if isinstance(value, dict):
                for k, v in value.items():
                    print(f"  {k}: {v}")
            else:
                print(f"  {value}")
        
        print("\nSample of merged data:")
        print(merged_df.head())
        
    except Exception as e:
        print(f"Error processing data: {str(e)}")

if __name__ == "__main__":
    main() 