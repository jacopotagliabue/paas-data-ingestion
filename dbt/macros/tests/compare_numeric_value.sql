{% test compare_numeric_value(model, column_name, gte=None) %}

SELECT *
FROM {{ model }}
WHERE
    {% if gte != None -%}
        {{ column_name }} < {{ gte }}
    {%- endif %}

{% endtest %}
