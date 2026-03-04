from config.config_paths import DATASETS_PATHS, OUTPUT_PATH
from pipeline.ingestion import ingestion_function
from pipeline.feature_engineering import feature_engineering_function
from pipeline.type_enforcement import type_enforcement_function
from pipeline.deduplication import deduplicate_function
from pipeline.key_validation import key_relationship_validation
from pipeline.aggregation import build_enriched_orders
from pipeline.metrics import run_metrics
from utils.etl_logger import pipeline_logger
from utils.decorators import log_and_time_step

logger = pipeline_logger()
log_step = log_and_time_step(logger)


def main():
    
    logger.info('Pipeline started')

    try:
## ingestion

        datasets = ingestion_function(DATASETS_PATHS)

    # Feature engineering

        datasets = feature_engineering_function(datasets)

    ## schema enforcement/ type enforcement
        
        datasets = type_enforcement_function(datasets)

    ## deduplicate
        
        datasets = deduplicate_function(datasets)

    ## Key relationship validation

        datasets = key_relationship_validation(datasets)
        

    ## Aggregate and JOIN

        enriched_dataset = build_enriched_orders(datasets)

        #print(enriched_dataset.head())

    ## Metrics
        run_metrics(enriched_dataset, datasets)

    except Exception:
        logger.exception(f'Pipeline failed')
        raise




if __name__ == "__main__":
    main()









