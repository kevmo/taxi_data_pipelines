{{ config(materialized="view")}}

select * from {{ source('staging', 'rides_green') }}
limit 100