#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module for extracting economic indicators from the World Bank API.
Fetches inflation, unemployment, GDP per capita, and interest rates data.
"""

import os
import time
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Tuple

import requests
import pandas as pd
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/world_bank_extractor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('world_bank_extractor')

# Load environment variables
load_dotenv()

# Constants
WORLD_BANK_API_URL = os.getenv('WORLD_BANK_API_URL', 'https://api.worldbank.org/v2')
MAX_RETRIES = 3
RETRY_DELAY = 5  # Seconds between retries

# Indicator codes
INDICATOR_CODES = {
    'inflation_rate': 'FP.CPI.TOTL.ZG',       # Inflation, consumer prices (annual %)
    'unemployment_rate': 'SL.UEM.TOTL.ZS',    # Unemployment, total (% of total labor force)
    'gdp_per_capita': 'NY.GDP.PCAP.CD',       # GDP per capita (current US$)
    'interest_rate': 'FR.INR.LEND',           # Lending interest rate (%)
    'gdp_growth': 'NY.GDP.MKTP.KD.ZG',        # GDP growth (annual %)
    'gross_capital_formation': 'NE.GDI.TOTL.ZS',  # Gross capital formation (% of GDP)
    'trade_balance': 'NE.RSB.GNFS.ZS',        # External balance on goods and services (% of GDP)
    'fdi_inflow': 'BX.KLT.DINV.WD.GD.ZS',    # Foreign direct investment, net inflows (% of GDP)
    'government_debt': 'GC.DOD.TOTL.GD.ZS',   # Central government debt, total (% of GDP)
    'international_reserves': 'FI.RES.TOTL.CD',  # Total reserves (includes gold, current US$)
    'education_expenditure': 'SE.XPD.TOTL.GD.ZS',  # Government expenditure on education (% of GDP)
    'research_expenditure': 'GB.XPD.RSDV.GD.ZS'  # Research and development expenditure (% of GDP)
}


class WorldBankExtractor:
    """
    Extractor for economic indicators from the World Bank API.
    """
    
    def __init__(self, api_url: Optional[str] = None):
        """
        Initialize the extractor.
        
        Args:
            api_url: World Bank API URL (optional)
        """
        self.api_url = api_url or WORLD_BANK_API_URL
        self.session = requests.Session()
    
    def get_iso3_country_code(self, iso2_code: str) -> str:
        """Convert 2-letter ISO code to 3-letter ISO code."""
        # Common mappings for the countries we're using
        iso2_to_iso3 = {
            'US': 'USA',
            'GB': 'GBR',
            'DE': 'DEU',
            'JP': 'JPN',
            'BR': 'BRA',
            'IN': 'IND',
            'ZA': 'ZAF',
            'FR': 'FRA',
            'CA': 'CAN',
            'CN': 'CHN'
        }
        
        return iso2_to_iso3.get(iso2_code, iso2_code)
    
    def get_countries(self) -> pd.DataFrame:
        """
        Get list of countries from World Bank.
        
        Returns:
            DataFrame with country information
        """
        logger.info("Getting list of countries from World Bank...")
        
        for attempt in range(MAX_RETRIES):
            try:
                url = f"{self.api_url}/country?format=json&per_page=300"
                response = self.session.get(url)
                response.raise_for_status()
                
                # World Bank API returns a list where the first element is metadata
                # and the second element is the actual data
                data = response.json()
                
                if isinstance(data, list) and len(data) > 1:
                    countries_data = data[1]
                    
                    # Extract relevant fields
                    countries = []
                    for country in countries_data:
                        # Skip aggregates and regions
                        if country.get('region', {}).get('value') == 'Aggregates':
                            continue
                            
                        countries.append({
                            'name': country.get('name', ''),
                            'iso_code': country.get('id', ''),
                            'region': country.get('region', {}).get('value', ''),
                            'capital_city': country.get('capitalCity', ''),
                            'longitude': country.get('longitude', ''),
                            'latitude': country.get('latitude', '')
                        })
                    
                    df = pd.DataFrame(countries)
                    logger.info(f"Retrieved {len(df)} countries")
                    return df
                else:
                    logger.warning("Unexpected API response format")
                    raise ValueError("Unexpected API response format")
                
            except Exception as e:
                logger.error(f"Attempt {attempt+1}/{MAX_RETRIES} failed: {str(e)}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    logger.error(f"Error getting countries after {MAX_RETRIES} attempts.")
                    raise
        
        return pd.DataFrame()  # Should never reach here due to the raise above
    
    def get_indicator_data(self, country_code: str, indicator_code: str, 
                          start_year: int = 2000, end_year: int = None) -> pd.DataFrame:
        """
        Get data for a specific indicator and country.
        
        Args:
            country_code: ISO country code
            indicator_code: World Bank indicator code
            start_year: Starting year for data (default: 2000)
            end_year: Ending year for data (default: current year)
            
        Returns:
            DataFrame with indicator data
        """
        # Set end_year to current year if not specified
        if end_year is None:
            end_year = datetime.now().year
        
        logger.info(f"Getting indicator {indicator_code} for country {country_code} "
                  f"from {start_year} to {end_year}...")
        
        for attempt in range(MAX_RETRIES):
            try:
                url = (f"{self.api_url}/country/{country_code}/indicator/{indicator_code}"
                      f"?format=json&date={start_year}:{end_year}&per_page=100")
                
                response = self.session.get(url)
                response.raise_for_status()
                
                data = response.json()
                
                if isinstance(data, list) and len(data) > 1:
                    indicator_data = data[1]
                    
                    # Extract relevant fields
                    records = []
                    for record in indicator_data:
                        # Skip entries with no value
                        if record.get('value') is None:
                            continue
                            
                        records.append({
                            'country': record.get('country', {}).get('value', ''),
                            'country_code': record.get('country', {}).get('id', ''),
                            'indicator_name': record.get('indicator', {}).get('value', ''),
                            'indicator_code': record.get('indicator', {}).get('id', ''),
                            'year': record.get('date', ''),
                            'value': record.get('value', None)
                        })
                    
                    df = pd.DataFrame(records)
                    
                    if df.empty:
                        logger.warning(f"No data found for indicator {indicator_code} in country {country_code}")
                    else:
                        logger.info(f"Retrieved {len(df)} records for indicator {indicator_code} in country {country_code}")
                    
                    return df
                else:
                    logger.warning("Unexpected API response format")
                    if 'message' in data:
                        logger.warning(f"API message: {data['message']}")
                    return pd.DataFrame()
                
            except Exception as e:
                logger.error(f"Attempt {attempt+1}/{MAX_RETRIES} failed: {str(e)}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    logger.error(f"Error getting indicator data after {MAX_RETRIES} attempts.")
                    return pd.DataFrame()  # Return empty DataFrame instead of raising exception
        
        return pd.DataFrame()
    
    def get_all_economic_indicators(self, country_codes: List[str], 
                                   start_year: int = 2000, end_year: int = None) -> pd.DataFrame:
        """
        Get all economic indicators for multiple countries.
        
        Args:
            country_codes: List of ISO country codes
            start_year: Starting year for data
            end_year: Ending year for data
            
        Returns:
            DataFrame with all economic indicators
        """
        all_data = []
        
        for country_code in country_codes:
            country_data = {}
            country_data['country_code'] = self.get_iso3_country_code(country_code)
            
            for indicator_name, indicator_code in INDICATOR_CODES.items():
                logger.info(f"Fetching {indicator_name} for {country_code}...")
                
                # Get data for this indicator
                df = self.get_indicator_data(country_code, indicator_code, start_year, end_year)
                
                if not df.empty:
                    # Extract yearly values
                    for _, row in df.iterrows():
                        year = row['year']
                        value = row['value']
                        
                        # Initialize year entry if needed
                        if year not in country_data:
                            country_data[year] = {}
                        
                        # Add indicator value
                        country_data[year][indicator_name] = value
                
                # Sleep to avoid hitting rate limits
                time.sleep(1)
            
            # Convert to records for this country
            for year, indicators in country_data.items():
                if year != 'country_code':  # Skip non-year entries
                    record = {
                        'country_code': country_data['country_code'],
                        'year': year
                    }
                    record.update(indicators)
                    all_data.append(record)
        
        # Convert to DataFrame
        if all_data:
            result_df = pd.DataFrame(all_data)
            
            # Convert year to datetime
            result_df['date'] = pd.to_datetime(result_df['year'], format='%Y')
            
            return result_df
        else:
            return pd.DataFrame()
    
    def export_to_csv(self, df: pd.DataFrame, filename: str) -> str:
        """
        Export DataFrame to CSV file.
        
        Args:
            df: DataFrame to export
            filename: Base name for the file
            
        Returns:
            Path to the saved file
        """
        if df.empty:
            logger.warning("DataFrame is empty, nothing to export")
            return ""
        
        # Create output directory if it doesn't exist
        output_dir = "data/economic_indicators/raw"
        os.makedirs(output_dir, exist_ok=True)
        
        # Add timestamp to filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = os.path.join(output_dir, f"{filename}_{timestamp}.csv")
        
        # Save to CSV
        df.to_csv(file_path, index=False)
        logger.info(f"Data exported to {file_path}")
        
        return file_path


if __name__ == "__main__":
    # Example usage
    extractor = WorldBankExtractor()
    
    # Get list of countries
    countries_df = extractor.get_countries()
    
    # Filter to a smaller set for testing
    test_countries = ['US', 'GB', 'DE', 'JP', 'BR', 'IN', 'ZA']
    
    # Get economic indicators for test countries
    data_df = extractor.get_all_economic_indicators(test_countries, start_year=2015)
    
    # Export to CSV
    if not data_df.empty:
        extractor.export_to_csv(data_df, "economic_indicators")
    
    print("Extraction complete.")