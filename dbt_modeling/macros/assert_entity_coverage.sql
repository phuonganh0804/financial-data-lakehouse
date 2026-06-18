{% macro assert_entity_coverage(source_name, model, entity_col) %}
-- Coverage check, shared by the per-source singular tests.
-- Returns expected entities that have NO rows in `model`. A test fails when any
-- come back — an expected symbol/series that was never ingested or dropped from
-- the universe entirely. 
-- Per-entity recency can't catch this: it only ranges over entities that already exist in the data.
with expected as (
    select entity_id as entity
    from {{ ref('expected_entities') }}
    where source = '{{ source_name }}'
),
actual as (
    select distinct {{ entity_col }} as entity
    from {{ model }}
)
select e.entity
from expected e
left join actual a on a.entity = e.entity
where a.entity is null
{% endmacro %}
