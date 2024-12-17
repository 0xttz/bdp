# Danish Municipality Data Analysis

This repository contains socio-economic and crime statistics data for Danish municipalities.

## Data Files

### Socio-Economic Data (`municipality_socio_economic.csv`)
Contains yearly municipality-level data from 2008-2023 including:
- Total population
- Average income
- Gini coefficient (inequality measure)
- Unemployment rate
- Share of higher educated population

### Crime Statistics (`transformed_crime_statistics.csv`)
Contains transformed crime statistics for Danish municipalities.

## Repository Structure

├── 1crime-data/                 # Crime statistics processing
│   ├── context/                 # Documentation and table structures
│   ├── mining_scripts/          # Data fetching and processing scripts
│   └── transformed_crime_statistics.csv  # Final crime dataset
│
├── 2socio/                      # Socio-economic data processing
│   ├── context/                 # Documentation and table structures
│   ├── mining_scripts/          # Data fetching and processing scripts
│   └── municipality_socio_economic.csv  # Final socio-economic dataset
│
└── 3utils/                      # Utility scripts for data aggregation

## Data Sources
The data is sourced from Danish public statistics databases and processed using custom mining scripts for each data category (population, income, education, etc.).
