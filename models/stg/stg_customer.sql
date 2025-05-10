with source as (
    select * from {{ ref("src_customer") }}
),

renamed as (
    select
        c_custkey       as customer_key,
        c_name          as customer_name,
        c_address       as customer_address,
        c_nationkey     as nation_key,
        regexp_replace(c_phone, '[^0-9]', '') as phone_cleaned,
        c_acctbal       as account_balance,
        c_mktsegment    as market_segment,
        c_comment       as comment
    from source
)

select * from renamed
