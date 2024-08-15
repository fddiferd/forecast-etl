
select 
    brand, 
    acquisition_main_item,
    acquisition_channel,
    sku,
    date,
    sum(cross_count) as cross_count,
    sum(cross_amount) as cross_amount,
    sum(upsell_count) as upsell_count,
    sum(upsell_amount) as upsell_amount
from production.metrics.vw_cross_up_sell
where date >= dateadd('year', -1, current_date())
group by all