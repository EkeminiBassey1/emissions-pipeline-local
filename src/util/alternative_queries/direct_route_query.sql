WITH RankedRows AS (
  SELECT
    *,
    COUNT(1) OVER (PARTITION BY response_unnest.rank) AS rank_count
  FROM
    `{$project_id}.{$dataset_id}.{$base_coors_wr_kilometriert}`,
    UNNEST(response) AS response_unnest
  WHERE
    response_unnest.rank IN (1, 2)
)
SELECT
  Land_von,
  PLZ_von,
  Land_nach,
  PLZ_nach,
  response_unnest.rank as rank,
  response_unnest.strassenDistanzGesamt,
  response_unnest.faehreDistanzGesamt,
  response_unnest.bahnDistanzGesamt
FROM RankedRows,
UNNEST(response) AS response_unnest
WHERE
  (rank_count = 1)
  OR (rank_count > 1 AND response_unnest.rank = 2)