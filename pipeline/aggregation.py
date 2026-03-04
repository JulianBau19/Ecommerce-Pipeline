import pandas as pd
from utils.decorators import log_and_time_step
from utils.etl_logger import pipeline_logger

logger = pipeline_logger()
log_step = log_and_time_step(logger)

@log_step
def build_enriched_orders(datasets: dict) -> pd.DataFrame:

    datasets = datasets["orders_details"].merge(
    datasets["orders_list"][["Order ID","Order Date","CustomerName","State","City"]],
    on = 'Order ID',
    how = 'left'
    )

    return datasets


