import pandas as pd
from utils.etl_logger import pipeline_logger
logger = pipeline_logger()

def ingestion_function(paths: dict) -> dict:

    datasets = {}

    for name, path in paths.items():
        datasets[name] = pd.read_csv(path, sep=',')
        logger.info(f"Loaded dataset {name} from {'/'.join(path.parts[-3:])}") ##only 3 last folders..


    logger.info(f"Total datasets loaded: {len(datasets)}")

    return datasets


# import pandas as pd

# def ingestion_function(orders_list_path,orders_details_path,sales_target_path):
#         orders_details = pd.read_csv(orders_details_path, sep=',')
#         orders_list = pd.read_csv(orders_list_path, sep=',')
#         sales_target = pd.read_csv(sales_target_path, sep=',')

#         return orders_list,orders_details,sales_target

## AFTER ARCHITECHTURE REFACTOR
