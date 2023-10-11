
{% if var('RechargeOnetimes') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
select coalesce(MAX(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

{% set table_name_query %}
{{set_table_name('%recharge%onetimes')}}    
{% endset %}  

{% set results = run_query(table_name_query) %}

{% if execute %}
    {# Return the first column #}
    {% set results_list = results.columns[0].values() %}
    {% set tables_lowercase_list = results.columns[1].values() %}
{% else %}
    {% set results_list = [] %}
    {% set tables_lowercase_list = [] %}
{% endif %}

{% for i in results_list %}
        {% if var('get_brandname_from_tablename_flag') %}
            {% set brand =i.split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
        {% else %}
            {% set brand = var('default_brandname') %}
        {% endif %}

        {% if var('get_storename_from_tablename_flag') %}
            {% set store =i.split('.')[2].split('_')[var('storename_position_in_tablename')] %}
        {% else %}
            {% set store = var('default_storename') %}
        {% endif %}

        {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
            {% set hr = var('raw_table_timezone_offset_hours')[i] %}
        {% else %}
            {% set hr = 0 %}
        {% endif %}

    select
        '{{brand}}' as brand,
        '{{store}}' as store,
        coalesce(cast(id as string),'NA') as id	,		
        coalesce(cast(address_id as string),'NA') as address_id	,		
        coalesce(cast(customer_id as string), 'NA') as customer_id	,		
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="created_at") }} as {{ dbt.type_timestamp() }}) as created_at,		
        {{extract_nested_value("external_product_id","ecommerce","string")}} as external_product_id_ecommerce,
        {{extract_nested_value("external_variant_id","ecommerce","string")}} as external_variant_id_ecommerce,
        next_charge_scheduled_at	,		
        price	,		
        product_title	,		
        cast(quantity as int64) as quantity	,		
        sku_override	,		
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,	
        variant_title	,			
        is_cancelled	,		
        presentment_currency	,		
        sku	,					
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
    from {{i}} 
        {{unnesting("external_product_id")}}
        {{unnesting("external_variant_id")}}
        {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        where {{daton_batch_runtime()}}  >= {{max_loaded}}
        --WHERE 1=1
        {% endif %}
    qualify dense_rank() over (partition by id,customer_id,address_id order by {{daton_batch_runtime()}} desc) = 1
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
