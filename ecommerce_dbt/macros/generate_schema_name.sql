{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is none -%}

        {{ target.schema }}

    {%- elif target.name == 'ci' -%}

        CI_{{ custom_schema_name | trim }}

    {%- elif target.name == 'prod' -%}

        PROD_{{ custom_schema_name | trim }}

    {%- elif target.name == 'dev' -%}

        DEV_{{ custom_schema_name | trim }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}