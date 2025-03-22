#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module for loading extracted data into the PostgreSQL database.
Handles data insertion and updates for countries and economic indicators.
"""

import os
import sys
import logging
from typing import Dict, List, Optional, Union, Tuple
from datetime import datetime

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, and_, or_, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from dotenv import load_dotenv

# Add root directory to path for importing models
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.database.create_database_schema import Country, EconomicData, ETLLog

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/database_loader.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('database_loader')

# Load environment variables
load_dotenv()

# Database configuration
DB_TYPE = os.getenv('DB_TYPE', 'postgresql')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'country_analysis')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')

# Construct connection URL
if DB_TYPE.lower() == 'sqlite':
    DB_URL = f"sqlite:///{DB_NAME}.db"
elif DB_TYPE.lower() == 'postgresql':
    DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
else:
    logger.error(f"Unsupported database type: {DB_TYPE}")
    raise ValueError(f"Unsupported database type: {DB_TYPE}")


class DatabaseLoader:
    """
    Class for loading data into the PostgreSQL database.
    """
    
    def __init__(self, db_url: Optional[str] = None):
        """
        Initialize the loader.
        
        Args:
            db_url: Database connection URL (optional)
        """
        self.db_url = db_url or DB_URL
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)
    
    def load_countries(self, countries_df: pd.DataFrame) -> Tuple[int, int, int]:
        """
        Load countries into the database.
        
        Args:
            countries_df: DataFrame with country information
            
        Returns:
            Tuple (inserted_count, updated_count, ignored_count)
        """
        if countries_df.empty:
            logger.warning("Countries DataFrame is empty, nothing to load")
            return (0, 0, 0)
        
        required_cols = ['name', 'iso_code']
        missing_cols = [col for col in required_cols if col not in countries_df.columns]
        
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return (0, 0, 0)
        
        session = self.Session()
        inserted, updated, ignored = 0, 0, 0
        
        try:
            for _, row in countries_df.iterrows():
                try:
                    # Check if country already exists
                    country = session.query(Country).filter_by(iso_code=row['iso_code']).first()
                    
                    if country:
                        # Update country if needed
                        updated_fields = False
                        
                        if country.name != row['name']:
                            country.name = row['name']
                            updated_fields = True
                        
                        if 'region' in row and row['region'] is not None and country.region != row['region']:
                            country.region = row['region']
                            updated_fields = True
                        
                        if 'continent' in row and row['continent'] is not None and country.continent != row['continent']:
                            country.continent = row['continent']
                            updated_fields = True
                        
                        if updated_fields:
                            country.updated_at = datetime.now()
                            updated += 1
                        else:
                            ignored += 1
                    else:
                        # Create new country
                        new_country = Country(
                            name=row['name'],
                            iso_code=row['iso_code'],
                            region=row.get('region'),
                            continent=row.get('continent')
                        )
                        session.add(new_country)
                        inserted += 1
                    
                    # Commit after every 50 countries to avoid long transactions
                    if (inserted + updated + ignored) % 50 == 0:
                        session.commit()
                
                except Exception as e:
                    logger.error(f"Error processing country {row.get('name', 'Unknown')}: {str(e)}")
                    session.rollback()
                    ignored += 1
            
            # Final commit
            session.commit()
            logger.info(f"Countries loaded: {inserted} inserted, {updated} updated, {ignored} ignored")
            
            # Log the process
            self._log_process(
                process_name="load_countries",
                status="success",
                records_processed=inserted + updated,
                execution_duration=None
            )
            
            return (inserted, updated, ignored)
        
        except Exception as e:
            session.rollback()
            logger.error(f"Error loading countries: {str(e)}")
            
            # Log the error
            self._log_process(
                process_name="load_countries",
                status="failed",
                error_message=str(e),
                records_processed=0,
                execution_duration=None
            )
            
            raise
        finally:
            session.close()
    
    # Update the load_economic_data method in database_loader.py

    def load_economic_data(self, data_df: pd.DataFrame) -> Tuple[int, int, int]:
        """
        Load economic indicators into the database.
        
        Args:
            data_df: DataFrame with economic indicator data
            
        Returns:
            Tuple (inserted_count, updated_count, ignored_count)
        """
        if data_df.empty:
            logger.warning("Economic data DataFrame is empty, nothing to load")
            return (0, 0, 0)
        
        required_cols = ['country_code', 'date']
        missing_cols = [col for col in required_cols if col not in data_df.columns]
        
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return (0, 0, 0)
        
        # Make sure date is in datetime format
        if not pd.api.types.is_datetime64_dtype(data_df['date']):
            try:
                data_df['date'] = pd.to_datetime(data_df['date'], errors='coerce')
            except Exception as e:
                logger.error(f"Error converting date column: {str(e)}")
                return (0, 0, 0)
        
        # Create a mapping of country codes to country_ids
        session = self.Session()
        country_mapping = {}
        
        try:
            countries = session.query(Country).all()
            # Store both the exact code and the code converted to uppercase for matching
            for country in countries:
                country_mapping[country.iso_code] = country.country_id
                # Also add uppercase version for case-insensitive matching
                country_mapping[country.iso_code.upper()] = country.country_id
            
            logger.info(f"Country mapping contains {len(countries)} countries")
            logger.info(f"Sample country mappings: {list(country_mapping.items())[:5]}")
            
            # Debug: Show what country codes we're looking for
            unique_codes = data_df['country_code'].unique()
            logger.info(f"Looking for country codes: {unique_codes}")
            
        except Exception as e:
            logger.error(f"Error querying countries: {str(e)}")
            session.close()
            return (0, 0, 0)
        
        # Process economic data
        inserted, updated, ignored = 0, 0, 0
        
        try:
            for _, row in data_df.iterrows():
                try:
                    country_code = row['country_code']
                    
                    # Try exact match first
                    if country_code in country_mapping:
                        country_id = country_mapping[country_code]
                    # Try uppercase match
                    elif country_code.upper() in country_mapping:
                        country_id = country_mapping[country_code.upper()]
                    # No match found
                    else:
                        # Let's dump the full mapping for debugging
                        logger.warning(f"Country code not found: {country_code}")
                        logger.info(f"Available country codes: {list(country_mapping.keys())[:20]}")
                        ignored += 1
                        continue
                    
                    date = row['date']
                    
                    # Check if record already exists
                    existing = session.query(EconomicData).filter(
                        and_(
                            EconomicData.country_id == country_id,
                            EconomicData.date == date
                        )
                    ).first()
                    
                    # Prepare data
                    data = {
                        'country_id': country_id,
                        'date': date,
                        'inflation_rate': row.get('inflation_rate'),
                        'unemployment_rate': row.get('unemployment_rate'),
                        'gdp_per_capita': row.get('gdp_per_capita'),
                        'interest_rate': row.get('interest_rate'),
                        'created_at': datetime.now()
                    }
                    
                    if existing:
                        # Update existing record if values are different
                        update_needed = False
                        
                        for key, value in data.items():
                            if key != 'created_at' and getattr(existing, key) != value:
                                setattr(existing, key, value)
                                update_needed = True
                        
                        if update_needed:
                            updated += 1
                        else:
                            ignored += 1
                    else:
                        # Insert new record
                        session.add(EconomicData(**data))
                        inserted += 1
                    
                    # Commit every 50 records
                    if (inserted + updated + ignored) % 50 == 0:
                        session.commit()
                
                except Exception as e:
                    logger.error(f"Error processing economic data for {country_code}, date {date}: {str(e)}")
                    session.rollback()
                    ignored += 1
            
            # Final commit
            session.commit()
            
            # Verify data was actually saved
            count = session.query(EconomicData).count()
            logger.info(f"Verification: {count} records in economic_data table after loading")
            
            logger.info(f"Economic data loaded: {inserted} inserted, {updated} updated, {ignored} ignored")
            
            # Log the process
            self._log_process(
                process_name="load_economic_data",
                status="success",
                records_processed=inserted + updated,
                execution_duration=None
            )
            
            return (inserted, updated, ignored)
        
        except Exception as e:
            session.rollback()
            logger.error(f"Error loading economic data: {str(e)}")
            
            # Log the error
            self._log_process(
                process_name="load_economic_data",
                status="failed",
                error_message=str(e),
                records_processed=0,
                execution_duration=None
            )
            
            raise
        finally:
            session.close()
    
    def _log_process(self, process_name: str, status: str,
                   error_message: Optional[str] = None,
                   records_processed: Optional[int] = None,
                   execution_duration: Optional[float] = None) -> None:
        """
        Log ETL process details.
        
        Args:
            process_name: Name of the process
            status: Status of the process ('success', 'failed', etc.)
            error_message: Error message if applicable
            records_processed: Number of records processed
            execution_duration: Execution duration in seconds
        """
        session = self.Session()
        try:
            log_entry = ETLLog(
                process_name=process_name,
                execution_time=datetime.now(),
                status=status,
                error_message=error_message,
                records_processed=records_processed,
                execution_duration=execution_duration
            )
            
            session.add(log_entry)
            session.commit()
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error logging process: {str(e)}")
        finally:
            session.close()


if __name__ == "__main__":
    # Example usage
    import sys
    from src.extractors.world_bank_extractor import WorldBankExtractor
    
    # Extract countries
    extractor = WorldBankExtractor()
    countries_df = extractor.get_countries()
    
    # Extract economic data for a few countries
    test_countries = ['US', 'GB', 'DE', 'JP', 'BR', 'IN', 'ZA']
    economic_df = extractor.get_all_economic_indicators(test_countries, start_year=2015)
    
    # Load into database
    loader = DatabaseLoader()
    
    print("Loading countries...")
    countries_result = loader.load_countries(countries_df)
    print(f"Countries: {countries_result[0]} inserted, {countries_result[1]} updated, {countries_result[2]} ignored")
    
    print("Loading economic data...")
    economic_result = loader.load_economic_data(economic_df)
    print(f"Economic data: {economic_result[0]} inserted, {economic_result[1]} updated, {economic_result[2]} ignored")
    
    print("Loading complete.")