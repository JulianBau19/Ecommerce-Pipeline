import pandas as pd



def build_enriched_orders(datasets: dict) -> pd.DataFrame:

    datasets = datasets["orders_details"].merge(
    datasets["orders_list"][["Order ID","Order Date","CustomerName","State","City"]],
    on = 'Order ID',
    how = 'left'
    )

    return datasets


