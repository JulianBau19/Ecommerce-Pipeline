
dedup_rules = {
    'orders_details': {
        'dedup_keys': ['Order ID', 'order_line_number'],
        'keep': 'first'
    },
    'orders_list': {
        'dedup_keys': ['Order ID'],
        'keep': 'first'
    },
    'sales_target': {
        'dedup_keys': ['Month of Order Date', 'Category'],
        'keep': 'last'  
    }
}
