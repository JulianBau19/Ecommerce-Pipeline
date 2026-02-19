from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "raw"

DATASETS_PATHS = {  ## dict : {str , pd.dataframe}
    "orders_list": DATA_DIR / "orders_list.csv",
    "orders_details": DATA_DIR / "orders_details.csv",
    "sales_target": DATA_DIR / "sales_target.csv"
}

OUTPUT_PATH = BASE_DIR / "data" / "processed"
