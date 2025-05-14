WITH parameters AS
(
SELECT
CONCAT(user_pseudo_id,' ', cast((select value.int_value from ds.event_params where key = 'ga_session_id') as string) ) as unique_session_id,
regexp_extract( (select value.string_value from unnest(event_params) where `key` = 'page_location'), r'[^\/]+$') as landing_page, -- to identify the initial page the user landed on
traffic_source.`source` as source,-- traffic source to see where the user originated from
traffic_source.medium as medium, -- traffic medium for analyzing marketing channels
traffic_source.name as campaign,-- campaign name to segment by marketing campaign
device.category as device_category,-- device category (mobile, desktop, etc.) to segment by device type
device.`language` as device_language,-- device language for language segmentation
device.operating_system as device_OS,-- device operating system for OS segmentation
device.mobile_brand_name as devise_mobile_brand,-- device mobile brand for mobile brand segmentation
geo.country as country -- user country for geographic segmentation
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` ds
WHERE event_name ='session_start'
),
events AS
(
SELECT
DATE(timestamp_micros(event_timestamp)) as event_session_date, -- event timestamp to build the chronology of user actions
event_name,-- event name to identify the funnel stage
CONCAT(user_pseudo_id,' ', cast((select value.int_value from ds.event_params where key = 'ga_session_id') as string) ) as unique_session_id
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` ds
WHERE event_name IN ('session_start', 'view_item', 'add_to_cart', 'begin_checkout', 'add_shipping_info',  'add_payment_info', 'purchase')
)
SELECT
e.event_session_date,
e.event_name,
p.unique_session_id,
p.landing_page,
p.source,
p.medium,
p.campaign,
p.device_category,
p.device_language,
p.device_OS,
p.devise_mobile_brand,
p.country
FROM parameters p
LEFT JOIN events e ON p.unique_session_id = e.unique_session_id
