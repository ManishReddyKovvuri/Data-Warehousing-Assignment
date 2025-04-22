{% macro date_safe(col) %}
    case
        when {{ col }}::text ~ '^\d{4}-\d{2}-\d{2}$' then to_date({{ col }}::text, 'YYYY-MM-DD')
        when {{ col }}::text ~ '^\d{2}-\d{2}-\d{4}$' then to_date({{ col }}::text, 'DD-MM-YYYY')
        else date '1957-01-01'
    end
{% endmacro %}
