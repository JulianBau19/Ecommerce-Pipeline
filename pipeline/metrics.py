import pandas as pd
import numpy as np
from config.config_paths import OUTPUT_PATH
from utils.etl_logger import pipeline_logger

logger = pipeline_logger()

def monthly_revenue_vs_monthly_sales_target(enriched_dataset: pd.DataFrame,sales_target: pd.DataFrame) -> pd.DataFrame:

    enriched_dataset['Month'] = enriched_dataset['Order Date'].dt.to_period('M')
    sales_target['Month'] = sales_target['Month of Order Date'].dt.to_period('M')

    monthly_revenue = (
        enriched_dataset
        .groupby(['Month','Category'], as_index=False)
        .agg({'Amount': 'sum'})
        .rename(columns={'Amount':'Revenue'})
        )
    
    enriched_dataset = monthly_revenue.merge(
        sales_target[['Month','Category','Target']],
        on = ['Month','Category'],
        how= 'left'

    )
    ## reach the target column
    enriched_dataset['Reach the target'] = np.where(
        enriched_dataset['Target'].notna() & 
        (enriched_dataset['Revenue'] > enriched_dataset['Target']),
        'Yes',
        'No'
    )

    ## % reached column 
    enriched_dataset['% reached'] = np.where(  ## np.where(condicion, valor_si_es_verdad, valor_si_es_mentira)
        enriched_dataset['Target'] > 0,
        round((enriched_dataset['Revenue'] * 100 / enriched_dataset['Target']),2), np.nan
    )

    return enriched_dataset


    
def monthly_revenue_growth_rate(enriched_dataset: pd.DataFrame)-> pd.DataFrame:

    enriched_dataset['Month'] = enriched_dataset['Order Date'].dt.to_period('M')

    monthly_revenue = (
        enriched_dataset
        .groupby('Month', as_index = False)
        .agg({'Amount': 'sum'})
        .rename(columns={'Amount':'Revenue'})
        #.sort_values('Month')
        )
    
    monthly_revenue['Mom %'] = (
        monthly_revenue['Revenue'].pct_change() * 100).round(2)
    
    return monthly_revenue


def top_5_products_montly_rev(enriched_dataset: pd.DataFrame)-> pd.DataFrame:

    enriched_dataset['Month'] = enriched_dataset['Order Date'].dt.to_period('M')
    df_grouped = (
        enriched_dataset
        .groupby(['Month','Sub-Category'], as_index= False)
        .agg({'Amount': 'sum'})
        .rename(columns={'Amount': 'Revenue'})
        .sort_values(by=['Month','Revenue'], ascending=[True,False])
    )
    top_5 = df_grouped.groupby('Month').head(5).reset_index(drop=True)
    top_5['Rank'] = top_5.groupby('Month')['Revenue'].rank(ascending=False, method='first').astype(int)

    return top_5
    

def avg_order_per_month(enriched_dataset: pd.DataFrame)-> pd.DataFrame:
    
    enriched_dataset['Month'] = enriched_dataset['Order Date'].dt.to_period('M')
    ## sum(revenue) per month

    df_grouped = (
        enriched_dataset
        .groupby(['Month', 'Order ID'], as_index= False)
        .agg({'Amount':'sum'})
        .rename(columns={'Amount': 'Revenue'})
    )

    count_orders = df_grouped.groupby('Month')['Order ID'].nunique()
    df = round((df_grouped['Revenue'].sum() / count_orders),2)


    return df


def run_metrics(enriched_dataset, datasets):

    df = monthly_revenue_vs_monthly_sales_target(
        enriched_dataset,
        datasets["sales_target"]
    )

    df.to_csv(OUTPUT_PATH / "Monthy_revenue_target_information.csv", index=False)
    logger.info("Monthly revenue vs target file saved.")

    df = monthly_revenue_growth_rate(enriched_dataset)
    df.to_csv(OUTPUT_PATH / "Monthy_revenue_growth_rate.csv", index=False)
    logger.info("Monthly revenue growth rate file saved.")

    df = top_5_products_montly_rev(enriched_dataset)
    df.to_csv(OUTPUT_PATH / "Top_5_productos_per_month_revenue.csv", index=False)
    logger.info("Top 5 products per month file saved.")

    df = avg_order_per_month(enriched_dataset)
    df.to_csv(OUTPUT_PATH / "Average_order_per_month.csv", index=False)
    logger.info("Average order value file saved.")