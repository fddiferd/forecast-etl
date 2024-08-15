
SELECT 
    LEFT(ar.cohort_start_date,7) as date, 
    ar.brand,
    CASE 
        WHEN p.recurring_price > 28.00
            AND p.recurring_price < 28.10
            AND pmv.trial_period_days = 0 
            AND pmv.recurring_period_days = 30 
            AND pmv.main_item = 'person_report' 
            THEN '28.05 - BG' 
        WHEN p.recurring_price > 28.32
            AND p.recurring_price < 28.34
            AND pmv.trial_period_days = 0 
            AND pmv.recurring_period_days = 30 
            AND pmv.main_item = 'person_report' 
            THEN '28.33 - BG' 
        WHEN p.recurring_price > 46.50
            AND p.recurring_price < 46.60
            AND pmv.trial_period_days = 0 
            AND pmv.recurring_period_days = 60 
            AND pmv.main_item = 'person_report'
            THEN '46.56 - BG' 
        WHEN p.recurring_price > 27.98
            AND p.recurring_price < 28.00 
            AND pmv.trial_period_days = 0 
            AND pmv.recurring_period_days = 30 
            AND pmv.main_item = 'person_report'
            THEN '27.99 - BG (Mobile)'
        WHEN p.recurring_price > 28.00
            AND p.recurring_price < 28.10
            AND pmv.trial_period_days = 30 
            AND pmv.recurring_period_days = 30 
            AND pmv.main_item = 'person_report'
            THEN '1mo 50% Discount - BG'
        WHEN pmv.recurring_period_days = 30 
            AND pmv.main_item = 'pdf_download'
            AND p.recurring_price > 3.97
            AND p.recurring_price < 4
            THEN 'PDF/DataMonitoring'
        WHEN pmv.recurring_period_days = 30 
            AND pmv.main_item != 'person_report'
            AND pmv.main_item != 'phone_report'
            THEN 'Other/Cross-sell (non phone/person)'
        WHEN p.recurring_price > 4.98
            AND p.recurring_price < 5
            AND pmv.recurring_period_days = 30 
            AND pmv.main_item = 'phone_report'
            THEN '4.99 - RPL'
        ELSE 'should not happen' END as sku_group,
    ar.cycle,
    DATEADD(MONTH,ar.cycle*(recurring_period_days/30),DATE(LEFT(ar.cohort_start_date,7)||'-01')) as bill_month,
    -- SPLIT_PART(ar.sku_bucket,'_',1) as product,
    SUM(txn_amount) as amount,
    SUM(eligible) as eligibles,
    SUM(success) as successes,
    DIV0(successes,eligibles) as retn_rate 
FROM dbt_environment.certified.activations_and_renewals ar -- ar on ar.cohort_start_date >= d.date  
JOIN dbt_environment.certified.int_pmv pmv on pmv.sku = ar.sku and pmv.brand_slug = ar.brand 
JOIN accounts.public.plans p on pmv.plan_id = p.id 
WHERE true -- LEFT(ar.rebill_date,4) = LEFT(d.date,4)
AND cycle < 19 
AND ar.brand in ('TruthFinder')
AND 
(( p.recurring_price > 28.00
AND p.recurring_price < 28.10
AND pmv.trial_period_days = 0 
AND pmv.recurring_period_days = 30 
AND pmv.main_item = 'person_report' ) 
OR
( p.recurring_price > 28.32
AND p.recurring_price < 28.34
AND pmv.trial_period_days = 0 
AND pmv.recurring_period_days = 30 
AND pmv.main_item = 'person_report' ) 
OR
( p.recurring_price > 46.50
AND p.recurring_price < 46.60
AND pmv.trial_period_days = 0 
AND pmv.recurring_period_days = 60 
AND pmv.main_item = 'person_report')
OR
( p.recurring_price > 27.98
AND p.recurring_price < 28.00 
AND pmv.trial_period_days = 0 
AND pmv.recurring_period_days = 30 
AND pmv.main_item = 'person_report')
OR
( p.recurring_price > 28.00
AND p.recurring_price < 28.10
AND pmv.trial_period_days = 30 
AND pmv.recurring_period_days = 30 
AND pmv.main_item = 'person_report')
OR
( pmv.recurring_period_days = 30 
AND pmv.main_item != 'person_report'
AND pmv.main_item != 'phone_report')
OR 
( pmv.recurring_period_days = 30 
AND pmv.main_item = 'pdf_download'
AND p.recurring_price > 3.97
AND p.recurring_price < 4 )
OR 
( p.recurring_price > 4.98
AND p.recurring_price < 5
AND pmv.recurring_period_days = 30 
AND pmv.main_item = 'phone_report'))
and ar.cohort_start_date >= '2022-01-01'
-- AND ar.cohort_start_date > '20'
GROUP BY 1,2,3,4,5
UNION
SELECT 
    LEFT(ar.cohort_start_date,7) as date, 
    ar.brand,
    term as sku_group,
    ar.cycle,
    DATEADD(MONTH,ar.cycle*(recurring_period_days/30),DATE(LEFT(ar.cohort_start_date,7)||'-01')) as bill_month,
    -- SPLIT_PART(ar.sku_bucket,'_',1) as product,
    SUM(txn_amount) as amount,
    SUM(eligible) as eligibles,
    SUM(success) as successes,
    DIV0(successes,eligibles) as retn_rate ,
    --  count(distinct pmv.sku),
    --   listagg(distinct pmv.sku,', ')
FROM dbt_environment.certified.activations_and_renewals ar -- ar on ar.cohort_start_date >= d.date  
JOIN dbt_environment.certified.int_pmv pmv on pmv.sku = ar.sku and pmv.brand_slug = ar.brand 
JOIN accounts.public.plans p on pmv.plan_id = p.id 
WHERE true -- LEFT(ar.rebill_date,4) = LEFT(d.date,4)
AND cycle < 19 
AND ar.brand in ('Intelius')
AND pmv.term in ('6d-trial-30d-34.95-phone',
'30d-24.86-person',
'30d-4.99-email',
'30d-4.99-phone',
'5d-trial-30d-29.63-person',
'30d-trial-30d-24.86-person',
'7d-trial-30d-34.95-location',
'7d-trial-30d-29.78-location',
'60d-42.25-person',
'30d-9.95-internet_monitoring',
'30d-19.99-person',
'30d-4.99-location',
'30d-24.99-person',
'30d-2.99-data_monitoring')
and ar.cohort_start_date >= '2022-01-01'
-- and pmv.sku not ilike 'mcc%'
-- AND ar.cohort_start_date > '20'
GROUP BY 1,2,3,4,5
UNION
SELECT 
    LEFT(ar.cohort_start_date,7) as date, 
    ar.brand,
    CASE 
        WHEN p.recurring_price > 35.11
            AND p.recurring_price < 35.13
            AND pmv.trial_period_days = 0 
            AND pmv.recurring_period_days = 30 
            AND pmv.main_item = 'person_report' 
            THEN '35.12 - BG' 
        WHEN p.recurring_price > 27.07
            AND p.recurring_price < 27.09
            AND pmv.trial_period_days = 0 
            AND pmv.recurring_period_days = 30 
            AND pmv.main_item = 'person_report' 
            THEN '27.08 - BG' 
        WHEN p.recurring_price > 84.27
            AND p.recurring_price < 84.29
            AND pmv.trial_period_days = 0 
            AND pmv.recurring_period_days = 90 
            AND pmv.main_item = 'person_report'
            THEN '84.28 - BG' 
        WHEN p.recurring_price > 34.98
            AND p.recurring_price < 35
            AND pmv.trial_period_days = 0 
            AND pmv.recurring_period_days = 30 
            AND pmv.main_item = 'person_report'
            THEN '34.99 - BG (Mobile)'
        WHEN p.recurring_price > 34.77
            AND p.recurring_price < 34.79
            AND pmv.trial_period_days = 30 
            AND pmv.recurring_period_days = 30 
            AND pmv.main_item = 'person_report'
            THEN '1mo 50% Discount - BG'
        WHEN p.recurring_price > 35.11
            AND p.recurring_price < 35.13
            AND pmv.trial_period_days > 0
            AND pmv.trial_period_days < 30
            AND p.trial_price > 0.99
            AND p.trial_price < 1.01
            AND pmv.recurring_period_days = 30 
            AND pmv.main_item = 'person_report'
            THEN '$1 Trial -> 35.12'
        WHEN pmv.recurring_period_days = 30 
            AND pmv.main_item = 'pdf_download'
            AND p.recurring_price > 3.97
            AND p.recurring_price < 4
            THEN 'PDF/DataMonitoring'
        WHEN pmv.recurring_period_days = 30 
            AND pmv.main_item != 'person_report'
            AND pmv.main_item != 'phone_report'
            AND pmv.main_item != 'pdf_download'
            THEN 'Other/Cross-sell (non phone/person)'
        WHEN p.recurring_price > 5.98
            AND p.recurring_price < 6
            AND pmv.recurring_period_days = 30 
            AND pmv.main_item = 'phone_report'
            THEN '5.99 - RPL'
        ELSE 'should not happen' END as sku_group,
        -- CASE WHEN subscription_type = 'cross' THEN 'cross' ELSE 'new' END as sub_type,
    ar.cycle,
    DATEADD(MONTH,ar.cycle*(recurring_period_days/30),DATE(LEFT(ar.cohort_start_date,7)||'-01')) as bill_month,
    -- SPLIT_PART(ar.sku_bucket,'_',1) as product,
    SUM(txn_amount) as amount,
    SUM(eligible) as eligibles,
    SUM(success) as successes,
    DIV0(successes,eligibles) as retn_rate 
    -- count(distinct pmv.sku),
    --     listagg(distinct pmv.sku,', ')
FROM dbt_environment.certified.activations_and_renewals ar -- ar on ar.cohort_start_date >= d.date  
JOIN dbt_environment.certified.int_pmv pmv on pmv.sku = ar.sku and pmv.brand_slug = ar.brand 
JOIN accounts.public.plans p on pmv.plan_id = p.id 
WHERE true -- LEFT(ar.rebill_date,4) = LEFT(d.date,4)
AND cycle < 19 
AND ar.brand in ('InstantCheckmate')
AND 
(        ( p.recurring_price > 35.11
            AND p.recurring_price < 35.13
            AND pmv.trial_period_days = 0 
            AND pmv.recurring_period_days = 30 
            AND pmv.main_item = 'person_report' 
            ) OR 
        ( p.recurring_price > 27.07
            AND p.recurring_price < 27.09
            AND pmv.trial_period_days = 0 
            AND pmv.recurring_period_days = 30 
            AND pmv.main_item = 'person_report' 
            ) 
            OR 
        ( p.recurring_price > 84.27
            AND p.recurring_price < 84.29
            AND pmv.trial_period_days = 0 
            AND pmv.recurring_period_days = 90 
            AND pmv.main_item = 'person_report'
            ) OR 
        ( p.recurring_price > 34.98
            AND p.recurring_price < 35
            AND pmv.trial_period_days = 0 
            AND pmv.recurring_period_days = 30 
            AND pmv.main_item = 'person_report'
            ) OR 
        ( p.recurring_price > 34.77
            AND p.recurring_price < 34.79
            AND pmv.trial_period_days = 30 
            AND pmv.recurring_period_days = 30 
            AND pmv.main_item = 'person_report'
            ) OR 
        ( p.recurring_price > 35.11
            AND p.recurring_price < 35.13
            AND pmv.trial_period_days > 0
            AND pmv.trial_period_days < 30
            AND p.trial_price > 0.99
            AND p.trial_price < 1.01
            AND pmv.recurring_period_days = 30 
            AND pmv.main_item = 'person_report'
            ) OR 
        ( pmv.recurring_period_days = 30 
            AND pmv.main_item = 'pdf_download'
            AND p.recurring_price > 3.97
            AND p.recurring_price < 4
            ) OR 
        ( pmv.recurring_period_days = 30 
            AND pmv.main_item != 'person_report'
            AND pmv.main_item != 'phone_report'
            AND pmv.main_item != 'pdf_download'
            ) OR 
        ( p.recurring_price > 5.98
            AND p.recurring_price < 6
            AND pmv.recurring_period_days = 30 
            AND pmv.main_item = 'phone_report'
            ) 
        )
and ar.cohort_start_date >= '2022-01-01'
-- and pmv.sku not ilike 'mcc%'
-- AND ar.cohort_start_date > '20'
GROUP BY 1,2,3,4,5
ORDER BY 2,3,1,4