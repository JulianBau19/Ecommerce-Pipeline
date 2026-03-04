from config.deduplication_rules import dedup_rules
from utils.decorators import log_and_time_step
from utils.etl_logger import pipeline_logger

logger = pipeline_logger()
log_step = log_and_time_step(logger)

@log_step
def deduplicate_function(datasets: dict) -> dict:

    for name, df in datasets.items():

        rules = dedup_rules.get(name)

        if rules:
            cleaned_df = df.drop_duplicates(
                subset=rules["dedup_keys"],
                keep=rules["keep"]
            )

            deleted_rows = len(df) - len(cleaned_df)
            logger.info(f'Deleted in {name}: {deleted_rows} rows')

            datasets[name] = cleaned_df

        else:
            logger.warning(f'No deduplication rules for {name}')

    return datasets
