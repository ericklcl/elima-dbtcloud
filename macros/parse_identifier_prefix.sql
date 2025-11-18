{% macro parse_identifier_prefix(raw_text) %}
    CASE
        WHEN regexp_substr({{ raw_text }}::string,
                           '^[[:space:]]*([A-Za-z0-9]+)[[:space:]]*:',
                           1, 1, 'e', 1)
             IN ('PO','SO','CN','VT','SI','F8','TIP','KL','TP','MH','TPN','DN','ZH', 'OI','TN','SU','MB','DK','MI','IL','GZ','INFO','E2OId','RF','CB','DJ','AO')
        THEN regexp_substr({{ raw_text }}::string,
                           '^[[:space:]]*([A-Za-z0-9]+)[[:space:]]*:',
                           1, 1, 'e', 1)
        ELSE NULL
    END
{% endmacro %}