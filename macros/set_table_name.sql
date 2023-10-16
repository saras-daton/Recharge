 {% if target.type =='snowflake' %}
 
    select concat(table_catalog,'.',table_schema, '.',table_name) as tables,
    lower(concat(table_catalog,'.',table_schema, '.',table_name)) as tables_lowercase
    from {{ var('raw_database') }}.INFORMATION_SCHEMA.TABLES 
    where lower(table_name) like '{{variable}}'  and table_schema='{{ var("raw_schema") }}'
    {% else %}
    select concat(table_catalog,'.',table_schema, '.',table_name) as tables,
    lower(concat(table_catalog,'.',table_schema, '.',table_name)) as tables_lowercase
    from {{ var('raw_database') }}.{{ var('raw_schema') }}.INFORMATION_SCHEMA.TABLES
    where lower(table_name) like '{{variable}}'
    {% endif %}

{% endmacro %}