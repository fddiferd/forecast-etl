select 
    date,
    brand,
    sku_type,
    channel,
    sum(advertising_spend) as advertising_spend,
    sum(new_sales) as new_sales,
    sum(resubscribe_sales) as resubscribe_sales,
    sum(transactor_sales) as transactor_sales
from dbt_environment.certified.daily_sales_report group by all

-- ;

-- select 
--     sum(advertising_spend),
--     sum(new_sales),
--     sum(resubscribe_sales),
--     sum(transactor_sales)

-- from dbt_environment.certified.daily_sales_report
-- where date ilike '2023-01%'
-- and channel = 'Affiliate'
-- and sku_type = 'BG'
-- and brand = 'InstantCheckmate'