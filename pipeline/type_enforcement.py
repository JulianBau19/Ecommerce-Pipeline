import pandas as pd
import numpy as np

def type_enforcement_function(datasets: dict) -> dict:

    # orders_details
    if "orders_details" in datasets:
        df = datasets["orders_details"]

        df["Order ID"] = df["Order ID"].astype(str)
        assert df["Order ID"].notna().all(), \
            "Null values detected in orders_details - Order ID"

        if "order_line_number" in df.columns:
            df["order_line_number"] = df["order_line_number"].astype(str)

        numeric_cols = ["Amount", "Profit", "Quantity"]
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="raise")

        datasets["orders_details"] = df

    # orders_list

    if "orders_list" in datasets:
        df = datasets["orders_list"]

        df["Order ID"] = df["Order ID"].astype(str)
        df["Order ID"] = df["Order ID"].replace("nan", np.nan)
        df = df.dropna(subset=["Order ID"]).copy()

        assert df["Order ID"].notna().all(), \
            "Null values detected in orders_list - Order ID"

        df["Order Date"] = pd.to_datetime(df["Order Date"],format="%d-%m-%Y",errors="raise")

        datasets["orders_list"] = df

    # sales_target

    if "sales_target" in datasets:
        df = datasets["sales_target"]

        df["Category"] = df["Category"].astype(str)
        df["Month of Order Date"] = pd.to_datetime(df["Month of Order Date"],format="%b-%y",errors="raise")
        df["Target"] = pd.to_numeric(df["Target"], errors="raise")
        datasets["sales_target"] = df

    return datasets




