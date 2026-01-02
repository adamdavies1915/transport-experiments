# NOLA Transit Scraper

An independent data collection system for New Orleans RTA real-time transit data, capturing streetcar and bus positions via SSE and storing them as Parquet files in Cloudflare R2. Built to analyze streetcar delays in mixed traffic versus dedicated right-of-way segments, providing independent data to verify or challenge RTA performance claims. Includes DuckDB queries for analyzing delay patterns across different route segments. Created out of curiosity about whether St. Charles streetcar delays correlate with traffic conditions on Canal Street.
