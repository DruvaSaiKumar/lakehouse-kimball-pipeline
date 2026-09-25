{% test value_in_range(model, column_name, min_value, max_value) %}
{#- Fails for every row outside [min_value, max_value]. Written inline so the project needs no packages. -#}
select {{ column_name }} as value
from {{ model }}
where {{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }}
{% endtest %}
