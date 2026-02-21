from config.config_paths import DATASETS_PATHS, OUTPUT_PATH
from pipeline.ingestion import ingestion_function
from pipeline.type_enforcement import type_enforcement_function
from pipeline.deduplication import deduplicate_function
from pipeline.key_validation import key_relationship_validation
from pipeline.aggregation import build_enriched_orders
from pipeline.metrics import monthly_revenue_vs_monthly_sales_target, monthly_revenue_growth_rate, top_5_products_montly_rev, avg_order_per_month
from utils.etl_logger import pipeline_logger

logger = pipeline_logger()

def main():
    logger.info('Pipeline started')
    try:
## ingestion

        datasets = ingestion_function(DATASETS_PATHS)

    # Feature engineering

        if "orders_details" in datasets:
            df = datasets["orders_details"]

            df["order_line_number"] = (df.groupby("Order ID").cumcount() + 1)

            df = df[["order_line_number"] + [col for col in df.columns if col != "order_line_number"]]

            datasets["orders_details"] = df


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

        df = (monthly_revenue_vs_monthly_sales_target(enriched_dataset,datasets['sales_target']))
        df.to_csv(OUTPUT_PATH / "Monthy_revenue_target_information.csv", index=False)
        logger.info("Monthly revenue vs target file saved.")

        df = (monthly_revenue_growth_rate(enriched_dataset).head())
        df.to_csv(OUTPUT_PATH / "Monthy_revenue_growth_rate.csv", index=False)
        logger.info("Monthly revenue growth rate file saved.")

        df =(top_5_products_montly_rev(enriched_dataset))
        df.to_csv(OUTPUT_PATH / "Top_5_productos_per_month_revenue.csv", index=False)
        logger.info("Top 5 products per month file saved.")

        df = (avg_order_per_month(enriched_dataset))
        df.to_csv(OUTPUT_PATH / "Average_order_per_month.csv", index=False)
        logger.info("Average order value file saved.")

        logger.info("Pipeline finished successfully")

    except Exception:
        logger.exception(f'Pipeline failed')
        raise











if __name__ == "__main__":
    main()









