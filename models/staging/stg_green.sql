{{ config(materialized="view")}}

select * from {{ source('staging', 'rides_green')}}