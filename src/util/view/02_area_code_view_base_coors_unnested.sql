CREATE OR REPLACE VIEW `{$project_id}.{$dataset_id}.base_coors_wr_kilometriert_excel_file` AS
SELECT
  ID,
  Land_von,
  Plz_von,
  Land_nach,
  Plz_nach,
  eventTimestamp,
  response_unnest.rank as rank,
  response_unnest.strassenDistanzGesamt,
  response_unnest.faehreDistanzGesamt,
  response_unnest.bahnDistanzGesamt,
  response_unnest.manoeuvres,
  response_unnest.walterRoutenInfos.walterRoutenId,
  response_unnest.walterRoutenInfos.istBevorzugt,
  response_unnest.walterRoutenInfos.laendersperren,
  response_unnest.walterRoutenInfos.beschreibung,
  response_unnest.walterRoutenInfos.orgEinheit,
  response_unnest.walterRoutenInfos.geografieVon.land as geografieVon_land,
  response_unnest.walterRoutenInfos.geografieVon.plzZonen as geografieVon_plzZonen,
  response_unnest.walterRoutenInfos.geografieNach.land as geografieNach_land,
  response_unnest.walterRoutenInfos.geografieNach.plzZonen as geografieNach_plzZonen,
  routenPunkte_unnest.laufendeNr,
  routenPunkte_unnest.typ,
  routenPunkte_unnest.ursprungKoordinate.x as ursprungKoordinate_x,
  routenPunkte_unnest.ursprungKoordinate.y as ursprungKoordinate_y,
  routenPunkte_unnest.berechneteKoordinate.x as berechneteKoordinate_x,
  routenPunkte_unnest.berechneteKoordinate.y as berechneteKoordinate_y,
  routenPunkte_unnest.fahrzeit,
  routenPunkte_unnest.distanzTyp,
  routenPunkte_unnest.distanz,
  routenPunkte_unnest.polygon,
  maut_unnested.landCode as maut_landCode,
  maut_unnested.landName as maut_landName,
  maut_unnested.kosten as maut_kosten,
  maut_unnested.strecke as maut_strecke,
  maut_tollTypes_unnest.tollType as maut_tollTypes_tollType,
  maut_tollTypes_unnest.strecke as maut_tollTypes_strecke,
  maut_tollTypes_unnest.kosten as maut_tollTypes_kosten,
  routenPunkte_unnest.viaPunkte,
  routenPunkte_unnest.land,
  routenPunkte_unnest.ort,
  routenPunkte_unnest.plz,
  routenPunkte_unnest.mnemoKuerzel,
  routenPunkte_unnest.ursprungTyp,
  mautGesamt_unnest.landCode as mautGesamt_landCode,
  mautGesamt_unnest.landName as mautGesamt_landName,
  mautGesamt_unnest.kosten as mautGesamt_kosten,
  mautGesamt_unnest.strecke as mautGesamt_strecke,
  mautGesamt_tollTypes_unnest.tollType as mautGesamt_tollTypes_tollType,
  mautGesamt_tollTypes_unnest.strecke as mautGesamt_tollTypes_strecke,
  mautGesamt_tollTypes_unnest.kosten as mautGesamt_tollTypes_kosten,
  anzahlGefundenerRouten
FROM `{$project_id}.{$dataset_id}.base_coors_wr_kilometriert_current_view`,
  UNNEST(response) AS response_unnest,
  UNNEST(response_unnest.routenPunkte) AS routenPunkte_unnest,
  UNNEST(response_unnest.mautGesamt) AS mautGesamt_unnest,
  UNNEST(mautGesamt_unnest.tollTypes) as mautGesamt_tollTypes_unnest,
  UNNEST(routenPunkte_unnest.maut) AS maut_unnested,
  UNNEST(maut_unnested.tollTypes) as maut_tollTypes_unnest