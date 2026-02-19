from utils.etl_logger import pipeline_logger
logger = pipeline_logger()


def key_relationship_validation(datasets: dict) -> dict:

    # primary key check
    if 'orders_list' in datasets:
        df = datasets['orders_list']

        duplicate_keys = df['Order ID'].duplicated().sum()

        if duplicate_keys > 0:
            logger.warning(f'{duplicate_keys} duplicated Order ID in orders_list')
    
    # foreing key ( orders_details to orders_list)

    if 'orders_details' in datasets:
        details_df = datasets['orders_details']
        header_df = datasets['orders_list'] ## father table 

        valid_keys = set(header_df['Order ID']) ## set is type structure optimized for large datasets

        orphan_mask = ~details_df["Order ID"].isin(valid_keys)
        orphan_count = orphan_mask.sum()


        if orphan_count > 0:

            orphan_revenue = details_df.loc[orphan_mask, 'Amount'].sum()
            
            logger.warning(f'{orphan_count} orphan records found in orders_details')
            logger.warning(f'Revenue Impact: {orphan_revenue}')
            details_df = details_df.loc[~orphan_mask].copy()
            datasets["orders_details"] = details_df

    return datasets


        