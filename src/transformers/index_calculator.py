#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module for calculating composite indices based on economic indicators and other data.
Creates the economic prosperity index, political stability index, and investment opportunity index.
"""

import os
import sys
import logging
from typing import Dict, List, Optional, Union, Tuple
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, and_, or_, func, desc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Add root directory to path for importing models
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.database.create_database_schema import Country, EconomicData, CalculatedIndex, ETLLog

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/index_calculator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('index_calculator')

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


class IndexCalculator:
    """
    Class for calculating indices based on economic and other indicators.
    """
    
    def __init__(self, db_url: Optional[str] = None):
        """
        Initialize the calculator.
        
        Args:
            db_url: Database connection URL (optional)
        """
        self.db_url = db_url or DB_URL
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)
    
    def _convert_country_codes(self, country_codes: List[str]) -> List[str]:
        """Convert 2-letter to 3-letter ISO codes if needed."""
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
        
        converted_codes = []
        for code in country_codes:
            if len(code) == 2 and code in iso2_to_iso3:
                converted_codes.append(iso2_to_iso3[code])
            else:
                converted_codes.append(code)
        
        return converted_codes

    def get_economic_data(self, country_codes: Optional[List[str]] = None, 
                        start_date: Optional[str] = None,
                        end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Retrieve economic data from the database.
        
        Args:
            country_codes: List of country ISO codes (None = all countries)
            start_date: Start date in 'YYYY-MM-DD' format (None = all dates)
            end_date: End date in 'YYYY-MM-DD' format (None = all dates)
            
        Returns:
            DataFrame with economic data
        """
        session = self.Session()
        try:
            if country_codes:
                country_codes = self._convert_country_codes(country_codes)
                logger.info(f"Converted country codes: {country_codes}")
                
            # Start query
            query = session.query(
                Country.country_id,
                Country.name,
                Country.iso_code,
                EconomicData.date,
                EconomicData.inflation_rate,
                EconomicData.unemployment_rate,
                EconomicData.gdp_per_capita,
                EconomicData.interest_rate,
                EconomicData.gdp_growth,
                EconomicData.gross_capital_formation,
                EconomicData.trade_balance,
                EconomicData.fdi_inflow,
                EconomicData.government_debt,
                EconomicData.international_reserves,
                EconomicData.education_expenditure,
                EconomicData.research_expenditure
            ).join(
                EconomicData, 
                Country.country_id == EconomicData.country_id
            )
            
            # Apply filters
            if country_codes:
                query = query.filter(Country.iso_code.in_(country_codes))
            
            if start_date:
                start_dt = pd.to_datetime(start_date)
                query = query.filter(EconomicData.date >= start_dt)
            
            if end_date:
                end_dt = pd.to_datetime(end_date)
                query = query.filter(EconomicData.date <= end_dt)
            
            # Order by country and date
            query = query.order_by(Country.iso_code, EconomicData.date)
            
            # Execute query and convert to DataFrame
            records = []
            for row in query.all():
                records.append({
                    'country_id': row.country_id,
                    'country_name': row.name,
                    'country_code': row.iso_code,
                    'date': row.date,
                    'inflation_rate': row.inflation_rate,
                    'unemployment_rate': row.unemployment_rate,
                    'gdp_per_capita': row.gdp_per_capita,
                    'interest_rate': row.interest_rate,
                    'gdp_growth': row.gdp_growth,
                    'gross_capital_formation': row.gross_capital_formation,
                    'trade_balance': row.trade_balance,
                    'fdi_inflow': row.fdi_inflow,
                    'government_debt': row.government_debt,
                    'international_reserves': row.international_reserves,
                    'education_expenditure': row.education_expenditure,
                    'research_expenditure': row.research_expenditure
                })
            
            df = pd.DataFrame(records)
            logger.info(f"Retrieved {len(df)} economic data records")
            return df
            
        except Exception as e:
            logger.error(f"Error retrieving economic data: {str(e)}")
            return pd.DataFrame()
        finally:
            session.close()
    
    def calculate_economic_prosperity_index(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate the Economic Prosperity Index.
        
        Formula: 
        - Normalize GDP per capita (higher is better)
        - Normalize inflation rate (lower is better)
        - Normalize unemployment rate (lower is better)
        - Normalize interest rate (lower is better)
        - Combine with weights into a 0-100 index
        
        Args:
            df: DataFrame with economic indicators
            
        Returns:
            DataFrame with calculated index
        """
        if df.empty:
            logger.warning("Economic data DataFrame is empty, cannot calculate index")
            return pd.DataFrame()
        
        # Check required columns
        required_cols = ['country_id', 'country_code', 'date']
        indicator_cols = [
            'gdp_per_capita', 'inflation_rate', 'unemployment_rate', 'interest_rate',
            'gdp_growth', 'gross_capital_formation', 'trade_balance', 'fdi_inflow',
            'government_debt', 'international_reserves', 'education_expenditure',
            'research_expenditure'
        ]
        
        missing_required = [col for col in required_cols if col not in df.columns]
        if missing_required:
            logger.error(f"Missing required columns: {missing_required}")
            return pd.DataFrame()
        
        # Make a copy of the input DataFrame
        result_df = df.copy()
        
        # Fill NaN values with the mean for each country
        for col in indicator_cols:
            if col in result_df.columns:
                # Group by country and fill NaN with mean
                result_df[col] = result_df.groupby('country_code')[col].transform(
                    lambda x: x.fillna(x.mean())
                )
        
        # For any remaining NaN values (if a country has all NaN for an indicator),
        # fill with global mean
        for col in indicator_cols:
            if col in result_df.columns:
                global_mean = result_df[col].mean()
                result_df[col] = result_df[col].fillna(global_mean)
        
        # Normalize indicators to 0-100 scale
        
        # 1. GDP per capita (higher is better)
        if 'gdp_per_capita' in result_df.columns:
            min_gdp = result_df['gdp_per_capita'].min()
            max_gdp = result_df['gdp_per_capita'].max()
            if max_gdp > min_gdp:
                result_df['gdp_normalized'] = 100 * (result_df['gdp_per_capita'] - min_gdp) / (max_gdp - min_gdp)
            else:
                result_df['gdp_normalized'] = 50
        else:
            result_df['gdp_normalized'] = 50
        
        # 2. GDP Growth (higher is better, but extreme values are concerning)
        if 'gdp_growth' in result_df.columns:
            # Score is highest at 3% growth, decreases in both directions
            result_df['gdp_growth_normalized'] = 100 - 15 * np.abs(result_df['gdp_growth'] - 3)
            result_df['gdp_growth_normalized'] = result_df['gdp_growth_normalized'].clip(0, 100)
        else:
            result_df['gdp_growth_normalized'] = 50
        
        # 3. Inflation rate (optimal around 2%)
        if 'inflation_rate' in result_df.columns:
            result_df['inflation_normalized'] = 100 - 10 * np.abs(result_df['inflation_rate'] - 2)
            result_df['inflation_normalized'] = result_df['inflation_normalized'].clip(0, 100)
        else:
            result_df['inflation_normalized'] = 50
        
        # 4. Unemployment rate (lower is better)
        if 'unemployment_rate' in result_df.columns:
            min_unemp = result_df['unemployment_rate'].min()
            max_unemp = result_df['unemployment_rate'].max()
            if max_unemp > min_unemp:
                result_df['unemployment_normalized'] = 100 * (1 - (result_df['unemployment_rate'] - min_unemp) / (max_unemp - min_unemp))
            else:
                result_df['unemployment_normalized'] = 50
        else:
            result_df['unemployment_normalized'] = 50
        
        # 5. Interest rate (optimal around 3-4%)
        if 'interest_rate' in result_df.columns:
            result_df['interest_normalized'] = 100 - 8 * np.abs(result_df['interest_rate'] - 3.5)
            result_df['interest_normalized'] = result_df['interest_normalized'].clip(0, 100)
        else:
            result_df['interest_normalized'] = 50
        
        # 6. Gross Capital Formation (higher is better)
        if 'gross_capital_formation' in result_df.columns:
            min_gcf = result_df['gross_capital_formation'].min()
            max_gcf = result_df['gross_capital_formation'].max()
            if max_gcf > min_gcf:
                result_df['gcf_normalized'] = 100 * (result_df['gross_capital_formation'] - min_gcf) / (max_gcf - min_gcf)
            else:
                result_df['gcf_normalized'] = 50
        else:
            result_df['gcf_normalized'] = 50
        
        # 7. Trade Balance (balanced is better)
        if 'trade_balance' in result_df.columns:
            result_df['trade_normalized'] = 100 - 5 * np.abs(result_df['trade_balance'])
            result_df['trade_normalized'] = result_df['trade_normalized'].clip(0, 100)
        else:
            result_df['trade_normalized'] = 50
        
        # 8. FDI Inflow (higher is better)
        if 'fdi_inflow' in result_df.columns:
            min_fdi = result_df['fdi_inflow'].min()
            max_fdi = result_df['fdi_inflow'].max()
            if max_fdi > min_fdi:
                result_df['fdi_normalized'] = 100 * (result_df['fdi_inflow'] - min_fdi) / (max_fdi - min_fdi)
            else:
                result_df['fdi_normalized'] = 50
        else:
            result_df['fdi_normalized'] = 50
        
        # 9. Government Debt (lower is better, but too low might be suboptimal)
        if 'government_debt' in result_df.columns:
            result_df['debt_normalized'] = 100 - np.maximum(0, (result_df['government_debt'] - 60) / 2)
            result_df['debt_normalized'] = result_df['debt_normalized'].clip(0, 100)
        else:
            result_df['debt_normalized'] = 50
        
        # 10. International Reserves (higher is better)
        if 'international_reserves' in result_df.columns:
            min_reserves = result_df['international_reserves'].min()
            max_reserves = result_df['international_reserves'].max()
            if max_reserves > min_reserves:
                result_df['reserves_normalized'] = 100 * (result_df['international_reserves'] - min_reserves) / (max_reserves - min_reserves)
            else:
                result_df['reserves_normalized'] = 50
        else:
            result_df['reserves_normalized'] = 50
        
        # 11. Education Expenditure (higher is better)
        if 'education_expenditure' in result_df.columns:
            min_edu = result_df['education_expenditure'].min()
            max_edu = result_df['education_expenditure'].max()
            if max_edu > min_edu:
                result_df['education_normalized'] = 100 * (result_df['education_expenditure'] - min_edu) / (max_edu - min_edu)
            else:
                result_df['education_normalized'] = 50
        else:
            result_df['education_normalized'] = 50
        
        # 12. Research Expenditure (higher is better)
        if 'research_expenditure' in result_df.columns:
            min_research = result_df['research_expenditure'].min()
            max_research = result_df['research_expenditure'].max()
            if max_research > min_research:
                result_df['research_normalized'] = 100 * (result_df['research_expenditure'] - min_research) / (max_research - min_research)
            else:
                result_df['research_normalized'] = 50
        else:
            result_df['research_normalized'] = 50
        
        # Calculate the economic prosperity index with updated weights
        result_df['economic_prosperity_index'] = (
            0.15 * result_df['gdp_normalized'] +           # GDP per capita
            0.10 * result_df['gdp_growth_normalized'] +    # GDP growth
            0.10 * result_df['inflation_normalized'] +     # Inflation
            0.10 * result_df['unemployment_normalized'] +  # Unemployment
            0.05 * result_df['interest_normalized'] +      # Interest rate
            0.10 * result_df['gcf_normalized'] +          # Gross Capital Formation
            0.05 * result_df['trade_normalized'] +        # Trade Balance
            0.10 * result_df['fdi_normalized'] +          # FDI Inflow
            0.05 * result_df['debt_normalized'] +         # Government Debt
            0.05 * result_df['reserves_normalized'] +     # International Reserves
            0.10 * result_df['education_normalized'] +    # Education Expenditure
            0.05 * result_df['research_normalized']       # Research Expenditure
        )
        
        # Round to 2 decimal places
        result_df['economic_prosperity_index'] = round(result_df['economic_prosperity_index'], 2)
        
        # For now, we'll skip political stability and investment opportunity indices
        # since they require more data we don't have yet
        result_df['political_stability_index'] = None
        result_df['investment_opportunity_index'] = None
        
        # Keep only necessary columns
        output_columns = [
            'country_id', 'country_code', 'country_name', 'date', 
            'economic_prosperity_index', 'political_stability_index', 'investment_opportunity_index'
        ]
        output_df = result_df[output_columns]
        
        logger.info(f"Economic prosperity index calculated for {len(output_df)} records")
        return output_df
    
    def save_calculated_indices(self, df: pd.DataFrame) -> Tuple[int, int, int]:
        """
        Save calculated indices to the database.
        
        Args:
            df: DataFrame with calculated indices
            
        Returns:
            Tuple (inserted_count, updated_count, ignored_count)
        """
        if df.empty:
            logger.warning("Calculated indices DataFrame is empty, nothing to save")
            return (0, 0, 0)
        
        required_cols = ['country_id', 'date', 'economic_prosperity_index']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return (0, 0, 0)
        
        session = self.Session()
        inserted, updated, ignored = 0, 0, 0
        
        try:
            for _, row in df.iterrows():
                try:
                    country_id = row['country_id']
                    date = row['date']
                    
                    # Check if record already exists
                    existing = session.query(CalculatedIndex).filter(
                        and_(
                            CalculatedIndex.country_id == country_id,
                            CalculatedIndex.date == date
                        )
                    ).first()
                    
                    # Prepare data
                    data = {
                        'country_id': country_id,
                        'date': date,
                        'economic_prosperity_index': row['economic_prosperity_index'],
                        'political_stability_index': row.get('political_stability_index'),
                        'investment_opportunity_index': row.get('investment_opportunity_index'),
                        'news_sentiment_index': row.get('news_sentiment_index'),
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
                        session.add(CalculatedIndex(**data))
                        inserted += 1
                    
                    # Commit every 50 records
                    if (inserted + updated + ignored) % 50 == 0:
                        session.commit()
                
                except Exception as e:
                    logger.error(f"Error saving calculated index for country_id {country_id}, date {date}: {str(e)}")
                    session.rollback()
                    ignored += 1
            
            # Final commit
            session.commit()
            logger.info(f"Calculated indices saved: {inserted} inserted, {updated} updated, {ignored} ignored")
            
            return (inserted, updated, ignored)
        
        except Exception as e:
            session.rollback()
            logger.error(f"Error saving calculated indices: {str(e)}")
            raise
        finally:
            session.close()
    
    def calculate_and_save_indices(self, country_codes: Optional[List[str]] = None,
                                 start_date: Optional[str] = None,
                                 end_date: Optional[str] = None) -> Dict:
        """
        Complete process: get data, calculate indices, and save to database.
        
        Args:
            country_codes: List of country ISO codes
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format
            
        Returns:
            Dict with process results
        """
        start_time = datetime.now()
        
        try:
            # Get economic data
            economic_df = self.get_economic_data(country_codes, start_date, end_date)
            
            if economic_df.empty:
                message = "No economic data found for the specified parameters"
                logger.warning(message)
                return {
                    'status': 'warning',
                    'message': message,
                    'inserted': 0,
                    'updated': 0,
                    'ignored': 0,
                    'duration_seconds': (datetime.now() - start_time).total_seconds()
                }
            
            # Calculate indices
            indices_df = self.calculate_economic_prosperity_index(economic_df)
            
            if indices_df.empty:
                message = "Failed to calculate indices"
                logger.error(message)
                return {
                    'status': 'error',
                    'message': message,
                    'inserted': 0,
                    'updated': 0,
                    'ignored': 0,
                    'duration_seconds': (datetime.now() - start_time).total_seconds()
                }
            
            # Save calculated indices
            inserted, updated, ignored = self.save_calculated_indices(indices_df)
            
            # Calculate duration
            duration = (datetime.now() - start_time).total_seconds()
            
            # Log the process
            self._log_process(
                process_name="calculate_and_save_indices",
                status="success",
                records_processed=inserted + updated,
                execution_duration=duration
            )
            
            return {
                'status': 'success',
                'message': f"Process completed successfully",
                'inserted': inserted,
                'updated': updated,
                'ignored': ignored,
                'duration_seconds': duration
            }
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"Error in calculate_and_save_indices: {str(e)}")
            
            # Log the error
            self._log_process(
                process_name="calculate_and_save_indices",
                status="failed",
                error_message=str(e),
                records_processed=0,
                execution_duration=duration
            )
            
            return {
                'status': 'error',
                'message': str(e),
                'inserted': 0,
                'updated': 0,
                'ignored': 0,
                'duration_seconds': duration
            }
    
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
    calculator = IndexCalculator()
    
    # Process for a subset of countries
    test_countries = ['US', 'GB', 'DE', 'JP', 'BR', 'IN', 'ZA']
    result = calculator.calculate_and_save_indices(
        country_codes=test_countries,
        start_date='2015-01-01'
    )
    
    print(f"Result: {result['status']}")
    print(f"Indices: {result['inserted']} inserted, {result['updated']} updated, {result['ignored']} ignored")
    print(f"Duration: {result['duration_seconds']:.2f} seconds")