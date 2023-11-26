from fastapi import FastAPI # type: ignore
from stock import StockDataCollector, StockMetadataManager  # Ensure these imports are correct
from config import constants as Config
import logging

app = FastAPI()

# Set up logging to print to stdout, which Cloud Run will capture
logging.basicConfig(level=logging.INFO)

# Initialize your classes outside of the endpoint
metadata_manager = StockMetadataManager(config=Config)
data_collector = StockDataCollector(metadata_manager=metadata_manager)

@app.post("/trigger_pipeline")
def trigger_pipeline():
    try:
        data, metadata = data_collector.run_ingestion_pipeline_localy()
        if data is not None and metadata is not None:
            logging.info("Data ingestion completed successfully.")
            
            # Attempt to save the updates, catching any exceptions that occur
            try:
                data_collector.save_updates(data=data, metadata=metadata)
                logging.info("Data saved successfully.")
                return {"message": "Data ingestion and save completed successfully."}
            except Exception as save_exception:
                logging.exception("Error occurred during data save.")
                return {"message": f"An error occurred during data save: {save_exception}"}

        else:
            logging.error("Data ingestion completed, but no data was returned.")
            return {"message": "Data ingestion completed, but no data was returned."}
    except Exception as e:
        logging.exception("Error occurred during data ingestion.")
        return {"message": f"An error occurred during data ingestion: {e}"}
