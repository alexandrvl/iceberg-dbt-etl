{# ============================================================================ #}
{# Data Vault 2.0 Hashing Macros for Trino                                   #}
{# ============================================================================ #}
{# These macros generate MD5 hash keys and hash diffs for Data Vault 2.0     #}
{# structures using Trino-compatible SQL                                      #}
{# ============================================================================ #}

{# Generate hash key for hubs and links #}
{% macro hash_key(columns, alias=none) -%}
    {%- if columns is string -%}
        {%- set columns = [columns] -%}
    {%- endif -%}

    {%- if columns | length == 1 -%}
        TO_HEX(MD5(TO_UTF8(COALESCE(CAST({{ columns[0] }} AS VARCHAR), ''))))
    {%- else -%}
        {%- set concat_string = [] -%}
        {%- for column in columns -%}
            {%- do concat_string.append("COALESCE(CAST(" ~ column ~ " AS VARCHAR), '')") -%}
        {%- endfor -%}
        TO_HEX(MD5(TO_UTF8(CONCAT({{ concat_string | join(", '||', ") }}))))
    {%- endif -%}
    {%- if alias %} AS {{ alias }}{% endif -%}
{%- endmacro %}

{# Generate hash diff for satellites to detect changes #}
{% macro hash_diff(columns, alias=none) -%}
    {%- if columns is string -%}
        {%- set columns = [columns] -%}
    {%- endif -%}

    {%- if columns | length == 1 -%}
        TO_HEX(MD5(TO_UTF8(COALESCE(CAST({{ columns[0] }} AS VARCHAR), ''))))
    {%- else -%}
        {%- set concat_string = [] -%}
        {%- for column in columns -%}
            {%- do concat_string.append("COALESCE(CAST(" ~ column ~ " AS VARCHAR), '')") -%}
        {%- endfor -%}
        TO_HEX(MD5(TO_UTF8(CONCAT({{ concat_string | join(", '||', ") }}))))
    {%- endif -%}
    {%- if alias %} AS {{ alias }}{% endif -%}
{%- endmacro %}
