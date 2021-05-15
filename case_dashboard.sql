-- pull main case management data
WITH base AS (
  SELECT
    ops.id,
    ops.teams_id,
    ops.type_id,
    ops.teams_name,
    ops.case_number,
    ops.owner_id,
    ops.status,
    ops.priority,
    DATE(FROM_UNIXTIME(ops.time_created)) time_created,
    DATE(FROM_UNIXTIME(ops.time_modified)) time_modified,
    DATE(FROM_UNIXTIME(metric.time_last_closed)) time_last_closed,
    ops.type_name,
    ops.field_array
  FROM operations ops
  LEFT JOIN operations_calculations metric
    ON ops.id = metric.id
  --JOIN TABLE FOR LEGACY CASES from xdb database
  LEFT JOIN xdb.ops xdbcase
    ON a.id = xdbcase.id
  WHERE
    ops.ds = '<LATEST_DS:operations>'
    AND metric.ds = '<LATEST_DS:operations_calculations>'
    AND xdbcase.legacy_id IS NULL -- remove all legacy tickets
    AND ops.type_id IN (
      123, -- CIA
      345, -- FBI
      678, -- NAVY
      901, --ARMY
    )
),
--Pull data from employee database
owner AS (
  SELECT
    id,
    first_name,
    last_name,
    owner_name
  FROM base
  INNER JOIN (
    SELECT
      personal_id owner_id,
      first_name,
      last_name,
      CONCAT(first_name, ' ', last_name) owner_name
    FROM employee
    WHERE
      ds = '<LATEST_DS:employee>'
  )
    USING (owner_id)
),
--PULL ALL INCIDENT WITH CASE_ID WITH TOOL TYPE 
incident_to_cases AS (
  SELECT
    id,
    investigation_id
  FROM incident_case
  WHERE
    ds = '<LATEST_DS:incident_case>'
    AND type = 'tool'
),
--PULL AND SEPARATE CASE FIELDS FROM ARRAY 
fields AS (
  SELECT
    id,
    REDUCE(
      field_array,
      ARRAY[],
      (s, x) -> IF (
          x.field_definition_id IN (
            111, -- Source
            222, -- Result
            333, -- Phase
          ),
          s || CAST(
            ROW(
              x.field_definition_id,
              COALESCE(x.field_value, ARRAY[])
            ) AS ROW(definition_id BIGINT, vals ARRAY(VARCHAR))
          ),
          s
        ),
      s -> MAP_FROM_ENTRIES(s)
    ) AS selected_fields
  FROM base
),
--COMBINE ALL TEMP TABLES INTO ONE
final_base AS (
  SELECT
    id,
    type_id,
    team_id,
    case_number,
    first_name,
    last_name,
    owner_id,
    owner_name,
    priority,
    status,
    team_name,
    time_created,
    time_modified,
    time_last_closed,
    try(selected_fields[111][1]) AS source,
    try(selected_fields[333][1]) AS phase,
    try(selected_fields[222][1]) AS result,
    investigation_id
  FROM base
  LEFT JOIN owner
    USING (id)
  LEFT JOIN fields
    USING (id)
  LEFT JOIN incident_to_cases
    USING (id)
),
--Calculation Function on assets
assets AS (
  SELECT
    investigation_id,
    COUNT(user_id) total_assets,
    COUNT(CASE
      WHEN user_type = 'victim' THEN 1
    END) AS victim_count,
    COUNT(CASE
      WHEN user_type = 'bad actor' THEN 1
    END) AS bad_actor_count,
  COUNT(CASE
      WHEN enforcement_type = 'disabled' THEN 1
    END) AS disabled_count,
    COUNT(CASE
      WHEN enforcement_type = 'monitoring' THEN 1
    END) AS monitoring_count,
      COUNT(CASE
      WHEN enforcement_type = 'deleted' THEN 1
    END) AS deleted_count,
  FROM asset_labels
  WHERE
    ds = '<LATEST_DS:asset_labels>'
  GROUP BY
    1
)
-- FINAL BASE TO ASSETS
SELECT
  base.id,
  base.team_id,
  base.type_id,
  base.case_number,
  base.preferred_first_name,
  base.preferred_last_name,
  base.owner_id,
  base.owner_name,
  base.priority,
  base.status,
  base.team_name,
  base.time_created,
  base.time_modified,
  base.time_last_closed,
  DATE_DIFF(base.time_last_closed, base.time_created, 'day') time_to_close
  base.source,
  base.phase,
  base.result,
  base.investigation_id,
  assets.total_assets,
  assets.victim_count,
  assets.bad_actor_count,
  assets.disabled_count,
  assets.monitoring_count,
  assets.deleted_count
FROM final_base base
LEFT JOIN assets assets
  ON base.investigation_id = assets.investigation_id
