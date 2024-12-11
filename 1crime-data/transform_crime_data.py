import pandas as pd

# Mapping municipalities to broader regions
REGION_MAPPING = {
    # Major Cities
    'Copenhagen': 'Copenhagen',
    'Aarhus': 'Aarhus',
    'Odense': 'Odense',
    'Aalborg': 'Aalborg',
    
    # Region names mapping
    'Region Hovedstaden': 'Hovedstaden',
    'Region Midtjylland': 'Midtjylland',
    'Region Nordjylland': 'Nordjylland',
    'Region Sjælland': 'Sjælland',
    'Region Syddanmark': 'Syddanmark',
    
    # Region Hovedstaden (Capital Region) - w/o Copenhagen
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
    
    # Sjælland 
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
    
    # Syddanmark w/o Odense
    'Assens': 'Syddanmark',
    'Billund': 'Syddanmark',
    'Esbjerg': 'Syddanmark',
    'Faaborg-Midtfyn': 'Syddanmark',
    'Fanø': 'Syddanmark',
    'Fredericia': 'Syddanmark',
    'Haderslev': 'Syddanmark',
    'Kerteminde': 'Syddanmark',
    'Kolding': 'Syddanmark',
    'Langeland': 'Syddanmark',
    'Middelfart': 'Syddanmark',
    'Nordfyns': 'Syddanmark',
    'Nyborg': 'Syddanmark',
    'Svendborg': 'Syddanmark',
    'Sønderborg': 'Syddanmark',
    'Tønder': 'Syddanmark',
    'Varde': 'Syddanmark',
    'Vejen': 'Syddanmark',
    'Vejle': 'Syddanmark',
    'Ærø': 'Syddanmark',
    'Aabenraa':'Syddanmark',
    
    # Midtjylland w/o Aarhus
    'Favrskov': 'Midtjylland',
    'Hedensted': 'Midtjylland',
    'Herning': 'Midtjylland',
    'Holstebro': 'Midtjylland',
    'Horsens': 'Midtjylland',
    'Ikast-Brande': 'Midtjylland',
    'Lemvig': 'Midtjylland',
    'Norddjurs': 'Midtjylland',
    'Odder': 'Midtjylland',
    'Randers': 'Midtjylland',
    'Ringkøbing-Skjern': 'Midtjylland',
    'Samsø': 'Midtjylland',
    'Silkeborg': 'Midtjylland',
    'Skanderborg': 'Midtjylland',
    'Skive': 'Midtjylland',
    'Struer': 'Midtjylland',
    'Syddjurs': 'Midtjylland',
    'Viborg': 'Midtjylland',
    
    # Nordjylland w/o Aalborg
    'Brønderslev': 'Nordjylland',
    'Frederikshavn': 'Nordjylland',
    'Hjørring': 'Nordjylland',
    'Jammerbugt': 'Nordjylland',
    'Læsø': 'Nordjylland',
    'Mariagerfjord': 'Nordjylland',
    'Morsø': 'Nordjylland',
    'Rebild': 'Nordjylland',
    'Thisted': 'Nordjylland',
    'Vesthimmerlands': 'Nordjylland',
    
    # Special cases
    'All Denmark': 'National',
    'Not stated municipality': 'Unknown',
    'Unknown municipality': 'Unknown',
    'Christiansø': 'Hovedstaden'
}

# Crime mappings
CRIME_CATEGORY_MAPPING = {
    # Violent Crimes
    'Assault causing actual bodily harm': 'Violent Crimes',
    'Common assault': 'Violent Crimes',
    'Unprovoked assault': 'Violent Crimes',
    'Particularly aggravated assault': 'Violent Crimes',
    'Violence against public authority': 'Violent Crimes',
    'Crimes of violence, total': 'Violent Crimes',
    'Offences against life and limb': 'Violent Crimes',
    'Causing death or bodily harm by negligence': 'Violent Crimes',
    'Intentional bodily harm': 'Violent Crimes',
    'Coercive control': 'Violent Crimes',
    'Coercive control etc': 'Violent Crimes',
    'Offences against personal liberty': 'Violent Crimes',
    'Any other kind of intentional trespass to the person': 'Violent Crimes',
    
    # Property Crimes
    'Burglary (uninhabited buildings)': 'Property Crimes',
    'Burglary - business and community': 'Property Crimes',
    'Residential burglaries': 'Property Crimes',
    'Malicious damage to property': 'Property Crimes',
    'Theft by finding': 'Property Crimes',
    'Theft from conveyances': 'Property Crimes',
    'Shoplifting, etc.': 'Property Crimes',
    'Other kinds of theft': 'Property Crimes',
    'Handling stolen goods': 'Property Crimes',
    'Receiving stolen goods by negligence': 'Property Crimes',
    'Theft of/taking bicycle without the owners consent (TWOC)': 'Property Crimes',
    'Theft of/taking moped without the owners consent (TWOC)': 'Property Crimes',
    'Theft of/taking vehicle without the owners consent (TWOC)': 'Property Crimes',
    'Theft of/taking other objects without the owners consent (TWOC)': 'Property Crimes',
    'Offences against property, total': 'Property Crimes',
    'Offence against and infringement of property': 'Property Crimes',
    'Arson': 'Property Crimes',
    
    # Economic Crimes
    'Aggravated tax evasion etc.': 'Economic Crimes',
    'Tax legislation and fiscal acts, etc.': 'Economic Crimes',
    'The Companies Act': 'Economic Crimes',
    'Fraud': 'Economic Crimes',
    'Fraud ': 'Economic Crimes',
    'Fraud against creditors': 'Economic Crimes',
    'Fraud against creditors ': 'Economic Crimes',
    'Fraud by abuse of position': 'Economic Crimes',
    'Fraud by cheque': 'Economic Crimes',
    'Fraud by cheque ': 'Economic Crimes',
    'Embezzlement ': 'Economic Crimes',
    'Forgery ': 'Economic Crimes',
    'Cheque forgery': 'Economic Crimes',
    'Cheque forgery ': 'Economic Crimes',
    'Blackmail and usury': 'Economic Crimes',
    'Offences concerning money and evidence': 'Economic Crimes',
    'Offences concerning money and evidence ': 'Economic Crimes',
    
    # Sexual Offenses
    'Rape, etc.': 'Sexual Offenses',
    'Sexual offenses, total': 'Sexual Offenses',
    'Sexual offence against a child under 12 (New from 2013)': 'Sexual Offenses',
    'Sexual offence against a child under 15 (New from 2013)': 'Sexual Offenses',
    'Any other kind of sexual offence (New from 2013)': 'Sexual Offenses',
    'Heterosexual offence against a child under 12 (Repealed in 2013)': 'Sexual Offenses',
    'Homosexual offence against a child under 12 (Repealed in 2013)': 'Sexual Offenses',
    'Any other kind of heterosexual offence (Repealed in 2013)': 'Sexual Offenses',
    'Any other kind of homosexual offence (Repealed in 2013)': 'Sexual Offenses',
    'Prostitution, etc.': 'Sexual Offenses',
    'Incest, etc.': 'Sexual Offenses',
    'Grooming': 'Sexual Offenses',
    'Offence against public decency by groping': 'Sexual Offenses',
    'Offence against public decency by indecent exposure': 'Sexual Offenses',
    'Any other kind of offence against public decency': 'Sexual Offenses',
    
    # Drug & Weapons Offenses
    'Euphoriants Act': 'Drug & Weapons Offenses',
    'Euphoriants Act ': 'Drug & Weapons Offenses',
    'Trafficking of drugs, etc.': 'Drug & Weapons Offenses',
    'Smuggling etc. of drugs': 'Drug & Weapons Offenses',
    'The Offensive Weapons Act': 'Drug & Weapons Offenses',
    
    # Public Order & Authority
    'Unlawful assembly/disturbance of public order': 'Public Order & Authority',
    'General public offences etc.': 'Public Order & Authority',
    'Offences against public authority, etc.': 'Public Order & Authority',
    'Offences by public servants': 'Public Order & Authority',
    'Offences by public servants ': 'Public Order & Authority',
    'Perjury': 'Public Order & Authority',
    'Invasion of privacy and defamation': 'Public Order & Authority',
    'Non-molestation order (Repealed in 2012)': 'Public Order & Authority',
    
    # Special Legislation
    'Building and housing legislation': 'Special Legislation',
    'Building and housing legislation ': 'Special Legislation',
    'Health and social security legislation': 'Special Legislation',
    'Health and social security legislation ': 'Special Legislation',
    'The environmental protection act': 'Special Legislation',
    'Legislation applying to public utilities': 'Special Legislation',
    'Legislation applying to public utilities ': 'Special Legislation',
    'Legislation on animals, hunting, etc.': 'Special Legislation',
    'Legislation on employment, transport, etc.': 'Special Legislation',
    'Legislation on gambling, licencing, trade': 'Special Legislation',
    'Legislation on gambling, licencing, trade ': 'Special Legislation',
    'Legislation on the national defence': 'Special Legislation',
    'Any other special legislation': 'Special Legislation',
    'Special acts, total': 'Special Legislation',
    'Special legislation, unspecified': 'Special Legislation',
    'Other special acts in criminal law': 'Special Legislation',
    'Family relation offences': 'Special Legislation',
    'Family relation offences ': 'Special Legislation',
    'Illegal trade, etc.': 'Special Legislation',
    
    # Remove
    'Criminal code, total': 'Totals',
    'Criminal code, unspecified': 'Totals',
    'Penal Code, total': 'Totals',
    'Penal code, unspecified': 'Totals',
    'Nature of the offence, total': 'Totals',
    'Other offences, total': 'Other',
    'Any other kind of false statement': 'Other',
    'Attempted homicide': 'Violent Crimes',
    'Homicide': 'Violent Crimes',
    'Involuntary manslaughter etc. in connection with traffic accident': 'Violent Crimes',
    'Threats': 'Violent Crimes',
    'Robbery': 'Property Crimes',
    'Embezzlement': 'Economic Crimes',
    'Forgery': 'Economic Crimes',
    'The Companies Act ': 'Economic Crimes',
    'Any other kind of false statement': 'Economic Crimes',
    'Other offences, total': 'Other'
}

def transform_data():
    df = pd.read_csv('final_crime_statistics.csv')
    columns_to_drop = ['processed_date', 'quarter', 'source', 'date']
    df = df.drop(columns=columns_to_drop, errors='ignore')
    
    df['municipality'] = df['region']  # Keep original region name
    df['broader_region'] = df['region'].map(REGION_MAPPING)
    
    df_grouped = df.groupby(['municipality', 'broader_region', 'year', 'quarter_num', 'offense_type'])['count'].sum().reset_index()
    
    df_grouped['crime_category'] = df_grouped['offense_type'].map(CRIME_CATEGORY_MAPPING)
    
    df_grouped = df_grouped[~df_grouped['crime_category'].isin(['Totals', 'Other'])]
    df_grouped = df_grouped[~df_grouped['broader_region'].isin(['Other', 'Unknown', 'National'])]
    df_grouped = df_grouped.sort_values(['year', 'quarter_num', 'broader_region', 'municipality'])
    
    df_grouped.to_csv('transformed_crime_statistics.csv', index=False)
    
    print("\nTransformation Summary:")
    print(f"Original columns: {len(df_grouped.columns)}")
    print("\nYear range:")
    print(f"From {df_grouped['year'].min()} to {df_grouped['year'].max()}")
    print("\nUnique crime categories:")
    print(df_grouped.groupby('crime_category')['count'].sum().sort_values(ascending=False))
    print("\nUnique broader regions:")
    print(df_grouped.groupby('broader_region')['count'].sum().sort_values(ascending=False))

if __name__ == "__main__":
    transform_data()