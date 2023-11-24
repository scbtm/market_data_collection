## Stock Data Ingestion Pipeline
This project is about ingesting stock data automatically in a parallelized way. The main function run_full_pipeline orchestrates the entire process.

Overview
The pipeline is designed to ingest data for a list of stock tickers. It fetches the data, processes it, and stores it in a specified location. The pipeline is designed to be efficient and robust, handling errors and ensuring data integrity.

Main Function
The run_full_pipeline function is the main driver of the pipeline. It takes as input a list of stock tickers, and existing data and metadata (if any). 

The function parallelizes the ingestion process by creating a separate process for each stock ticker. Each process fetches the data for its ticker, processes it, and stores it in the specified location. The function also handles errors and ensures data integrity.