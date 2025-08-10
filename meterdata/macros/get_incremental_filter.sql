{% macro get_incremental_filter(timestamp_column, lookback_hours=24) %}
  {% if is_incremental() %}
    -- Get records newer than the max timestamp in the target table
    -- Include a lookback period to handle late-arriving data
    where {{ timestamp_column }} > (
      select coalesce(max({{ timestamp_column }}), timestamp '1970-01-01 00:00:00') - interval '{{ lookback_hours }}' hour
      from {{ this }}
    )
  {% endif %}
{% endmacro %}