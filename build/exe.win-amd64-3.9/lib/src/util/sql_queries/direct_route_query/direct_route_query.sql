CREATE OR REPLACE VIEW `{$project_id}.{$dataset_id}.base_coors_wr_kilometriert_direct_route_view` AS(
SELECT
      ID,
      Land_von,
      PLZ_von,
      Land_nach,
      PLZ_nach,
      round((route.strassenDistanzGesamt / 1000) ,2) as StrasseDistanzGesamt, 
      round((route.faehreDistanzGesamt / 1000), 2) as FaehreDistanzGesamt, 
      round((route.bahnDistanzGesamt / 1000), 2) as BahnDistanzGesamt
  FROM
      `{$project_id}.{$dataset_id}.{$base_coors_wr_kilometriert}`,
      UNNEST(response) AS route
  WHERE
      route.rank = anzahlGefundenerRouten
)