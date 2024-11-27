{% macro get_updated_at(current_table, ref_table, unique_key, timestamp) %}
    case
        when {{ is_incremental() }} and exists (
            select 1 
            from {{ ref_table }}
            where {{ ref_table }}.{{ unique_key }} = {{ current_table }}.{{ unique_key }}
        )
        then (
            select updated_at 
            from {{ ref_table }} 
            where {{ ref_table }}.{{ unique_key }} = {{ current_table }}.{{ unique_key }}
            limit 1
        )
        else {{ timestamp }}
    end
{% endmacro %}
