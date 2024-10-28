CREATE OR REPLACE VIEW `{$project_id}.{$dataset_id}.{$excel_file_view_part}` AS(
SELECT * 
FROM `{$project_id}.{$dataset_id}.{$table_view}`
LIMIT {$parts} OFFSET {$offset}
)