select
    brand,
    sku,
    acquisition_sku,
    acquisition_channel,
    to_date(concat(date_part('year', date), '-', date_part('month', date), '-01'), 'yyyy-mm-dd') as date,
    sum(gross_adds) as gross_adds,
    sum(amount) as amount,
from production.metrics.vw_gross_adds
where date >= dateadd('year', -2, current_date())
group by all