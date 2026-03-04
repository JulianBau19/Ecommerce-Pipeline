from utils.decorators import log_and_time_step
from utils.etl_logger import pipeline_logger

logger = pipeline_logger()
log_step = log_and_time_step(logger)

@log_step
def feature_engineering_function(datasets: dict) -> dict:
        
        if "orders_details" in datasets:
            df = datasets["orders_details"]

            df["order_line_number"] = (df.groupby("Order ID").cumcount() + 1)

            df = df[["order_line_number"] + [col for col in df.columns if col != "order_line_number"]]

            datasets["orders_details"] = df
            
        return datasets
