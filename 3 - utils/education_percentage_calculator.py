import pandas as pd
import numpy as np
from pathlib import Path

def load_education_data(filepath: str) -> pd.DataFrame:
    """Load and preprocess education data"""
    try:
        df = pd.read_csv(filepath)        
        df['municipality'] = df['BOPOMR']
        df['education_level'] = df['HFUDD']
        df['year'] = pd.to_numeric(df['TID'])
        df['count'] = pd.to_numeric(df['INDHOLD'])
        
        # Select only needed columns
        df = df[['municipality', 'education_level', 'year', 'count']]
        
        return df
    except Exception as e:
        print(f"Error loading education data: {str(e)}")
        return pd.DataFrame()

def load_population_data(filepath: str) -> pd.DataFrame:
    """Load and preprocess population data"""
    try:
        df = pd.read_csv(filepath)
        
        # Convert year and population to numeric
        df['year'] = pd.to_numeric(df['year'])
        df['population'] = pd.to_numeric(df['population'])
        
        return df
    except Exception as e:
        print(f"Error loading population data: {str(e)}")
        return pd.DataFrame()

def calculate_education_percentages(edu_df: pd.DataFrame, pop_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate education percentages based on population"""
    try:
        # Merge education and population data
        merged_df = pd.merge(
            edu_df,
            pop_df,
            on=['municipality', 'year'],
            how='left'
        )
        
        # Calculate percentage
        merged_df['percentage'] = (merged_df['count'] / merged_df['population']) * 100
        
        # Round to 2 decimal places
        merged_df['percentage'] = merged_df['percentage'].round(2)
        
        # Sort the data
        merged_df = merged_df.sort_values(['municipality', 'year', 'education_level'])
        
        return merged_df
    except Exception as e:
        print(f"Error calculating percentages: {str(e)}")
        return pd.DataFrame()

def main():
    # Get the current working directory
    current_dir = Path.cwd().parent
    
    # Define file paths relative to the project root
    education_path = current_dir / "socio" / "mining_scripts" / "csvs-to-process" / "municipality_education.csv"
    population_path = current_dir / "socio" / "mining_scripts" / "csvs-to-process" / "municipality_population.csv"
    output_path = current_dir / "socio" / "mining_scripts" / "csvs-to-process" / "municipality_education_percentages.csv"
    
    print(f"Looking for education data at: {education_path}")
    print(f"Looking for population data at: {population_path}")
    
    print("\nLoading data...")
    
    # Load datasets
    edu_df = load_education_data(education_path)
    pop_df = load_population_data(population_path)
    
    if edu_df.empty or pop_df.empty:
        print("Failed to load data. Please check the error messages above.")
        return
    
    print("\nData loaded successfully!")
    print(f"Education data shape: {edu_df.shape}")
    print(f"Population data shape: {pop_df.shape}")
    
    print("\nCalculating percentages...")
    
    # Calculate percentages
    result_df = calculate_education_percentages(edu_df, pop_df)
    
    if result_df.empty:
        print("Failed to calculate percentages. Please check the error messages above.")
        return
    
    # Save results
    try:
        result_df.to_csv(output_path, index=False)
        print(f"\nResults saved to {output_path}")
        
        # Print summary statistics
        print("\nSummary Statistics:")
        print(f"Total records processed: {len(result_df)}")
        print("\nEducation levels found:")
        for level in sorted(result_df['education_level'].unique()):
            print(f"- {level}")
        
        print("\nSample of results:")
        print(result_df.head())
        
        # Print percentage ranges
        print("\nPercentage Ranges:")
        print(f"Min: {result_df['percentage'].min():.2f}%")
        print(f"Max: {result_df['percentage'].max():.2f}%")
        print(f"Mean: {result_df['percentage'].mean():.2f}%")
        
    except Exception as e:
        print(f"Error saving results: {str(e)}")

if __name__ == "__main__":
    main() 