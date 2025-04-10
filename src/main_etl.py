#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Main ETL script for the Global Country Analysis System.
Orchestrates the complete process: extraction, transformation, and loading.
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
from typing import List, Optional, Dict

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.extractors.world_bank_extractor import WorldBankExtractor
from src.loaders.database_loader import DatabaseLoader
from src.transformers.index_calculator import IndexCalculator
from src.database.create_database_schema import create_schema

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/main_etl.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('main_etl')


def initialize_database(drop_all: bool = False) -> bool:
    """
    Initialize the database schema.
    
    Args:
        drop_all: If True, drop all existing tables
        
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Initializing database (drop_all={drop_all})...")
    
    try:
        success = create_schema(drop_all=drop_all)
        
        if success:
            logger.info("Database initialized successfully")
        else:
            logger.error("Failed to initialize database")
        
        return success
    
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        return False


def run_extraction(countries: List[str], start_year: int = 2015) -> Dict:
    """
    Run the extraction process.
    
    Args:
        countries: List of country ISO codes
        start_year: Starting year for data extraction
        
    Returns:
        Dict with extraction results
    """
    logger.info(f"Starting extraction process for {len(countries)} countries from year {start_year}...")
    
    try:
        # Initialize extractor
        extractor = WorldBankExtractor()
        
        # Extract list of all countries
        all_countries_df = extractor.get_countries()
        
        if all_countries_df.empty:
            logger.error("Failed to extract countries list")
            return {
                'status': 'error',
                'message': 'Failed to extract countries list',
                'countries_df': None,
                'economic_df': None
            }
        
        logger.info(f"Extracted information for {len(all_countries_df)} countries")
        
        # Extract economic indicators for specified countries
        economic_df = extractor.get_all_economic_indicators(
            country_codes=countries,
            start_year=start_year
        )
        
        if economic_df.empty:
            logger.error(f"Failed to extract economic indicators for countries {countries}")
            return {
                'status': 'error',
                'message': 'Failed to extract economic indicators',
                'countries_df': all_countries_df,
                'economic_df': None
            }
        
        logger.info(f"Extracted {len(economic_df)} economic indicator records")
        
        return {
            'status': 'success',
            'message': 'Extraction completed successfully',
            'countries_df': all_countries_df,
            'economic_df': economic_df
        }
    
    except Exception as e:
        logger.error(f"Error during extraction: {str(e)}")
        return {
            'status': 'error',
            'message': f'Error during extraction: {str(e)}',
            'countries_df': None,
            'economic_df': None
        }


def run_loading(countries_df, economic_df) -> Dict:
    """
    Run the loading process.
    
    Args:
        countries_df: DataFrame with country information
        economic_df: DataFrame with economic indicators
        
    Returns:
        Dict with loading results
    """
    logger.info("Starting loading process...")
    
    try:
        # Initialize loader
        loader = DatabaseLoader()
        
        # Load countries
        if countries_df is not None and not countries_df.empty:
            countries_inserted, countries_updated, countries_ignored = loader.load_countries(countries_df)
            logger.info(f"Loaded countries: {countries_inserted} inserted, {countries_updated} updated, {countries_ignored} ignored")
        else:
            countries_inserted, countries_updated, countries_ignored = 0, 0, 0
            logger.warning("No countries data to load")
        
        # Load economic data
        if economic_df is not None and not economic_df.empty:
            economic_inserted, economic_updated, economic_ignored = loader.load_economic_data(economic_df)
            logger.info(f"Loaded economic data: {economic_inserted} inserted, {economic_updated} updated, {economic_ignored} ignored")
        else:
            economic_inserted, economic_updated, economic_ignored = 0, 0, 0
            logger.warning("No economic data to load")
        
        return {
            'status': 'success',
            'message': 'Loading completed successfully',
            'countries': {
                'inserted': countries_inserted,
                'updated': countries_updated,
                'ignored': countries_ignored
            },
            'economic_data': {
                'inserted': economic_inserted,
                'updated': economic_updated,
                'ignored': economic_ignored
            }
        }
    
    except Exception as e:
        logger.error(f"Error during loading: {str(e)}")
        return {
            'status': 'error',
            'message': f'Error during loading: {str(e)}'
        }


def run_index_calculation(countries: List[str], start_date: str) -> Dict:
    """
    Run the index calculation process.
    
    Args:
        countries: List of country ISO codes
        start_date: Start date in 'YYYY-MM-DD' format
        
    Returns:
        Dict with calculation results
    """
    logger.info(f"Starting index calculation for {len(countries)} countries from {start_date}...")
    
    try:
        # Initialize calculator
        calculator = IndexCalculator()
        
        # Calculate and save indices
        result = calculator.calculate_and_save_indices(
            country_codes=countries,
            start_date=start_date
        )
        
        logger.info(f"Index calculation result: {result['status']}")
        logger.info(f"Indices: {result['inserted']} inserted, {result['updated']} updated, {result['ignored']} ignored")
        
        return result
    
    except Exception as e:
        logger.error(f"Error during index calculation: {str(e)}")
        return {
            'status': 'error',
            'message': f'Error during index calculation: {str(e)}'
        }


def run_full_etl_process(countries: List[str], start_year: int = 2015, initialize_db: bool = False, drop_tables: bool = False) -> Dict:
    """
    Run the complete ETL process.
    
    Args:
        countries: List of country ISO codes
        start_year: Starting year for data extraction
        initialize_db: If True, initialize the database
        drop_tables: If True, drop existing tables when initializing
        
    Returns:
        Dict with process results
    """
    start_time = datetime.now()
    logger.info(f"Starting full ETL process at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    results = {
        'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
        'steps': {}
    }
    
    # Step 1: Initialize database if requested
    if initialize_db:
        db_success = initialize_database(drop_all=drop_tables)
        results['steps']['initialize_database'] = {
            'status': 'success' if db_success else 'error',
            'message': 'Database initialized successfully' if db_success else 'Failed to initialize database'
        }
        
        if not db_success:
            results['status'] = 'error'
            results['message'] = 'Failed to initialize database'
            results['duration_seconds'] = (datetime.now() - start_time).total_seconds()
            return results
    
    # Step 2: Run extraction
    logger.info("Running extraction step...")
    extraction_result = run_extraction(countries, start_year)
    results['steps']['extraction'] = extraction_result
    
    if extraction_result['status'] != 'success':
        results['status'] = 'error'
        results['message'] = f"Extraction failed: {extraction_result.get('message', 'Unknown error')}"
        results['duration_seconds'] = (datetime.now() - start_time).total_seconds()
        return results
    
    # Step 3: Run loading
    logger.info("Running loading step...")
    loading_result = run_loading(
        extraction_result['countries_df'],
        extraction_result['economic_df']
    )
    results['steps']['loading'] = loading_result
    
    if loading_result['status'] != 'success':
        results['status'] = 'error'
        results['message'] = f"Loading failed: {loading_result.get('message', 'Unknown error')}"
        results['duration_seconds'] = (datetime.now() - start_time).total_seconds()
        return results
    
    # Step 4: Run index calculation
    logger.info("Running index calculation step...")
    start_date = f"{start_year}-01-01"
    calculation_result = run_index_calculation(countries, start_date)
    results['steps']['index_calculation'] = calculation_result
    
    if calculation_result['status'] != 'success':
        results['status'] = 'error'
        results['message'] = f"Index calculation failed: {calculation_result.get('message', 'Unknown error')}"
        results['duration_seconds'] = (datetime.now() - start_time).total_seconds()
        return results
    
    # All steps completed successfully
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    results['status'] = 'success'
    results['message'] = 'Full ETL process completed successfully'
    results['end_time'] = end_time.strftime('%Y-%m-%d %H:%M:%S')
    results['duration_seconds'] = duration
    
    logger.info(f"Full ETL process completed successfully in {duration:.2f} seconds")
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run the Global Country Analysis ETL process')
    parser.add_argument('--countries', type=str, default='US,GB,DE,JP,BR,IN,ZA', 
                      help='Comma-separated list of country ISO codes (default: US,GB,DE,JP,BR,IN,ZA)')
    parser.add_argument('--start-year', type=int, default=2015,
                      help='Starting year for data extraction (default: 2015)')
    parser.add_argument('--init-db', action='store_true',
                      help='Initialize the database')
    parser.add_argument('--drop-tables', action='store_true',
                      help='Drop existing tables when initializing database')
    parser.add_argument('--extract-only', action='store_true',
                      help='Run only the extraction step')
    parser.add_argument('--calculate-only', action='store_true',
                      help='Run only the index calculation step')
    
    args = parser.parse_args()
    
    # Parse countries list
    country_list = [c.strip() for c in args.countries.split(',') if c.strip()]
    
    if args.extract_only:
        # Run extraction only
        result = run_extraction(country_list, args.start_year)
        print(f"Extraction result: {result['status']}")
        if result['economic_df'] is not None:
            print(f"Extracted {len(result['economic_df'])} economic data records")
        
    elif args.calculate_only:
        # Run index calculation only
        start_date = f"{args.start_year}-01-01"
        result = run_index_calculation(country_list, start_date)
        print(f"Index calculation result: {result['status']}")
        print(f"Indices: {result['inserted']} inserted, {result['updated']} updated, {result['ignored']} ignored")
        
    else:
        # Run full ETL process
        result = run_full_etl_process(
            countries=country_list,
            start_year=args.start_year,
            initialize_db=args.init_db,
            drop_tables=args.drop_tables
        )
        
        print(f"ETL process result: {result['status']}")
        print(f"Duration: {result['duration_seconds']:.2f} seconds")
        
        # Print step details if there were errors
        if result['status'] != 'success':
            for step, step_result in result['steps'].items():
                if step_result['status'] != 'success':
                    print(f"Error in {step} step: {step_result.get('message', 'Unknown error')}")