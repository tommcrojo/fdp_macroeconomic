#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script to create the database schema for the Global Country Analysis System.
This creates all the necessary tables in PostgreSQL.
"""

import os
import logging
import argparse
from datetime import datetime

import sqlalchemy as sa
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/database_schema.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('database_schema')

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

# Create base for ORM mapping classes
Base = declarative_base()

# Model definitions
class Country(Base):
    """
    Table for storing country information.
    """
    __tablename__ = 'country'
    
    country_id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    iso_code = Column(String(3), nullable=False, unique=True, index=True)
    region = Column(String(50))
    continent = Column(String(50))
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # Relationships
    economic_data = relationship("EconomicData", back_populates="country")
    freedom_data = relationship("FreedomIndex", back_populates="country")
    calculated_indices = relationship("CalculatedIndex", back_populates="country")
    
    def __repr__(self):
        return f"<Country(name='{self.name}', iso_code='{self.iso_code}')>"


class EconomicData(Base):
    """
    Table for storing economic indicators for countries.
    """
    __tablename__ = 'economic_data'
    
    data_id = Column(Integer, primary_key=True)
    country_id = Column(Integer, ForeignKey('country.country_id'), nullable=False)
    date = Column(DateTime, nullable=False)
    inflation_rate = Column(Float)
    unemployment_rate = Column(Float)
    gdp_per_capita = Column(Float)
    interest_rate = Column(Float)
    gdp_growth = Column(Float)  # GDP growth rate
    gross_capital_formation = Column(Float)  # Gross capital formation as % of GDP
    trade_balance = Column(Float)  # Trade balance as % of GDP
    fdi_inflow = Column(Float)  # Foreign direct investment inflow as % of GDP
    government_debt = Column(Float)  # Government debt as % of GDP
    international_reserves = Column(Float)  # International reserves in USD
    education_expenditure = Column(Float)  # Education expenditure as % of GDP
    research_expenditure = Column(Float)  # R&D expenditure as % of GDP
    created_at = Column(DateTime, default=datetime.now)
    
    # Relationship with Country
    country = relationship("Country", back_populates="economic_data")
    
    # Composite index for country_id and date
    __table_args__ = (sa.UniqueConstraint('country_id', 'date', name='uix_economic_data_country_date'),)
    
    def __repr__(self):
        return f"<EconomicData(country_id={self.country_id}, date='{self.date}')>"


class FreedomIndex(Base):
    """
    Table for storing freedom index data for countries.
    """
    __tablename__ = 'freedom_index'
    
    index_id = Column(Integer, primary_key=True)
    country_id = Column(Integer, ForeignKey('country.country_id'), nullable=False)
    year = Column(Integer, nullable=False)
    freedom_score = Column(Float)
    political_rights_score = Column(Float)
    civil_liberties_score = Column(Float)
    created_at = Column(DateTime, default=datetime.now)
    
    # Relationship with Country
    country = relationship("Country", back_populates="freedom_data")
    
    # Composite index for country_id and year
    __table_args__ = (sa.UniqueConstraint('country_id', 'year', name='uix_freedom_index_country_year'),)
    
    def __repr__(self):
        return f"<FreedomIndex(country_id={self.country_id}, year={self.year}, score={self.freedom_score})>"


class NewsArticle(Base):
    """
    Table for storing news articles related to countries.
    """
    __tablename__ = 'news_article'
    
    article_id = Column(Integer, primary_key=True)
    country_id = Column(Integer, ForeignKey('country.country_id'), nullable=False)
    date = Column(DateTime, nullable=False)
    title = Column(String(250), nullable=False)
    content = Column(Text)
    source = Column(String(100))
    url = Column(String(500))
    sentiment_score = Column(Float)
    created_at = Column(DateTime, default=datetime.now)
    
    def __repr__(self):
        return f"<NewsArticle(country_id={self.country_id}, date='{self.date}', title='{self.title[:20]}...')>"


class CalculatedIndex(Base):
    """
    Table for storing calculated indices for countries.
    """
    __tablename__ = 'calculated_index'
    
    index_id = Column(Integer, primary_key=True)
    country_id = Column(Integer, ForeignKey('country.country_id'), nullable=False)
    date = Column(DateTime, nullable=False)
    economic_prosperity_index = Column(Float)
    political_stability_index = Column(Float)
    investment_opportunity_index = Column(Float)
    news_sentiment_index = Column(Float)
    created_at = Column(DateTime, default=datetime.now)
    
    # Relationship with Country
    country = relationship("Country", back_populates="calculated_indices")
    
    # Composite index for country_id and date
    __table_args__ = (sa.UniqueConstraint('country_id', 'date', name='uix_calculated_index_country_date'),)
    
    def __repr__(self):
        return f"<CalculatedIndex(country_id={self.country_id}, date='{self.date}')>"


class ETLLog(Base):
    """
    Table for logging ETL processes.
    """
    __tablename__ = 'etl_log'
    
    log_id = Column(Integer, primary_key=True)
    process_name = Column(String(100), nullable=False)
    execution_time = Column(DateTime, nullable=False, default=datetime.now)
    status = Column(String(20), nullable=False)  # 'success', 'failed', etc.
    error_message = Column(String(500))
    records_processed = Column(Integer)
    execution_duration = Column(Float)  # Duration in seconds
    created_at = Column(DateTime, default=datetime.now)
    
    def __repr__(self):
        return f"<ETLLog(process='{self.process_name}', status='{self.status}')>"


def create_schema(engine_url: str = None, echo: bool = False, drop_all: bool = False):
    """
    Create the database schema.
    
    Args:
        engine_url: Database connection URL (optional)
        echo: If True, show generated SQL queries
        drop_all: If True, drop all existing tables before creating them
    """
    url = engine_url or DB_URL
    
    try:
        logger.info(f"Connecting to database: {url}")
        engine = create_engine(url, echo=echo)
        
        if drop_all:
            logger.warning("Dropping all existing tables...")
            Base.metadata.drop_all(engine)
        
        logger.info("Creating tables...")
        Base.metadata.create_all(engine)
        
        # Verify tables were created successfully
        inspector = sa.inspect(engine)
        created_tables = inspector.get_table_names()
        expected_tables = [
            'country', 'economic_data', 'freedom_index',
            'news_article', 'calculated_index', 'etl_log'
        ]
        
        missing_tables = [table for table in expected_tables if table not in created_tables]
        
        if missing_tables:
            logger.error(f"Could not create the following tables: {missing_tables}")
            return False
        else:
            logger.info(f"Schema created successfully. Tables: {created_tables}")
            return True
    
    except Exception as e:
        logger.error(f"Error creating schema: {str(e)}")
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create database schema for Global Country Analysis System')
    parser.add_argument('--echo', action='store_true', help='Show generated SQL queries')
    parser.add_argument('--drop', action='store_true', help='Drop existing tables before creating them')
    parser.add_argument('--url', type=str, help='Database connection URL (optional)')
    
    args = parser.parse_args()
    
    success = create_schema(
        engine_url=args.url,
        echo=args.echo,
        drop_all=args.drop
    )
    
    if success:
        print("Database schema created successfully.")
    else:
        print("Error creating database schema. See logs for more details.")