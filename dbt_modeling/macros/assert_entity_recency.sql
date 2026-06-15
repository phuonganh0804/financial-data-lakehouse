{% macro assert_entity_recency(model, entity_col, max_age_days, date_col='date_day', extra_cols=[]) %}
-- Per-entity recency check, shared by the per-source singular tests.
--
-- Returns one row per entity whose newest `date_col` is older than
-- `max_age_days` (a test fails when any rows come back). This closes the gap
-- that source freshness can't: source freshness is table-level
-- max(transformed_at), so a single symbol/series going stale while the others
-- keep the table fresh is invisible to it. This operates per `entity_col`.
--
-- `max_age_days` may be a literal (e.g. 3) or a SQL expression over the grouped
-- columns (e.g. a CASE on `frequency`). Any column the expression references
-- must be listed in `extra_cols` so it survives the aggregation.
with latest as (
    select
        {{ entity_col }} as entity,
        {%- for c in extra_cols %}
        max({{ c }}) as {{ c }},
        {%- endfor %}
        max({{ date_col }}) as latest_date
    from {{ model }}
    group by {{ entity_col }}
)

select
    entity,
    {%- for c in extra_cols %}
    {{ c }},
    {%- endfor %}
    latest_date,
    date_diff('day', latest_date, current_date) as age_days
from latest
where date_diff('day', latest_date, current_date) > ({{ max_age_days }})
{% endmacro %}
