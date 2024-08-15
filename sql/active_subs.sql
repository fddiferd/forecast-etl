select
    brand,
    acquisition_main_item as main_item,
    acquisition_channel as channel,
    sku,
    date,
    sum(subscription_count) as subscription_count,
    sum(amount) as amount,
    sum(amount) / sum(subscription_count) as arpu,


from production.metrics.vw_monthly_subs where date >= dateadd('year', -2, current_date())
group by all