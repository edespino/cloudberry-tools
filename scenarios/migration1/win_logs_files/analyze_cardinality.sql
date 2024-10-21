-- Function to get column cardinality
CREATE OR REPLACE FUNCTION get_column_cardinality(table_name text, column_name text)
RETURNS TABLE(total_rows bigint, distinct_values bigint, cardinality_ratio numeric) AS $$
BEGIN
    RETURN QUERY EXECUTE format('
        WITH stats AS (
            SELECT COUNT(*) AS total_rows,
                   COUNT(DISTINCT %I) AS distinct_values
            FROM %I
        )
        SELECT total_rows,
               distinct_values,
               (distinct_values::numeric / total_rows::numeric) AS cardinality_ratio
        FROM stats
    ', column_name, table_name);
END;
$$ LANGUAGE plpgsql;

-- Analyze cardinality for specific columns
SELECT 'boolean_col1' AS column_name, * FROM get_column_cardinality('win_logs', 'boolean_col1');
SELECT 'boolean_col2' AS column_name, * FROM get_column_cardinality('win_logs', 'boolean_col2');
SELECT 'text_col1' AS column_name, * FROM get_column_cardinality('win_logs', 'text_col1');
SELECT 'int_col1' AS column_name, * FROM get_column_cardinality('win_logs', 'int_col1');

-- Analyze cardinality for all columns (this might take a while for a large table)
SELECT column_name, *
FROM (
    SELECT column_name::text,
           (get_column_cardinality('win_logs', column_name::text)).*
    FROM information_schema.columns
    WHERE table_name = 'win_logs'
) subquery
ORDER BY cardinality_ratio ASC
LIMIT 10;  -- Show top 10 lowest cardinality columns

-- Drop the helper function
DROP FUNCTION IF EXISTS get_column_cardinality(text, text);
