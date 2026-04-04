-- ============================================================
-- Supplemental Analysis
-- NDH IG: https://build.fhir.org/ig/HL7/fhir-us-ndh/
-- Focused on CMS feedback areas:
--   1. Data processing strategy & normalization
--   2. Organization hierarchies
--   3. Practitioner → organization relationships
--   4. Source data combination
-- ============================================================


-- ------------------------------------------------------------
-- 1. RECORD COUNTS (confirmed post-ingestion)
-- ------------------------------------------------------------

SELECT 'organizations'          AS resource_type, source_dataset, COUNT(*) AS record_count FROM organizations          GROUP BY source_dataset
UNION ALL
SELECT 'organization_affiliations',               source_dataset, COUNT(*)               FROM organization_affiliations GROUP BY source_dataset
UNION ALL
SELECT 'practitioners',                           source_dataset, COUNT(*)               FROM practitioners             GROUP BY source_dataset
UNION ALL
SELECT 'practitioner_roles',                      source_dataset, COUNT(*)               FROM practitioner_roles        GROUP BY source_dataset
UNION ALL
SELECT 'locations',                               source_dataset, COUNT(*)               FROM locations                 GROUP BY source_dataset
UNION ALL
SELECT 'endpoints',                               source_dataset, COUNT(*)               FROM endpoints                 GROUP BY source_dataset
ORDER BY resource_type, source_dataset;


-- ------------------------------------------------------------
-- 2. NPI COVERAGE
-- Both datasets use different system URLs for NPI:
--   dataset-a: http://hl7.org/fhir/sid/us-npi
--   dataset-b: http://terminology.hl7.org/NamingSystem/npi
-- Both are checked to ensure accurate coverage counts.
-- ------------------------------------------------------------

-- Practitioner NPI coverage
SELECT
    source_dataset,
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE raw_json->'identifier' IS NOT NULL) AS has_any_identifier,
    COUNT(*) FILTER (WHERE raw_json->'identifier' @> '[{"system": "http://hl7.org/fhir/sid/us-npi"}]'
                       OR raw_json->'identifier' @> '[{"system": "http://terminology.hl7.org/NamingSystem/npi"}]') AS has_npi,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'identifier' @> '[{"system": "http://hl7.org/fhir/sid/us-npi"}]'
                       OR raw_json->'identifier' @> '[{"system": "http://terminology.hl7.org/NamingSystem/npi"}]') / COUNT(*), 1) AS pct_has_npi
FROM practitioners
GROUP BY source_dataset ORDER BY source_dataset;

-- Organization NPI coverage
SELECT
    source_dataset,
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE raw_json->'identifier' IS NOT NULL) AS has_any_identifier,
    COUNT(*) FILTER (WHERE raw_json->'identifier' @> '[{"system": "http://hl7.org/fhir/sid/us-npi"}]'
                       OR raw_json->'identifier' @> '[{"system": "http://terminology.hl7.org/NamingSystem/npi"}]') AS has_npi,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'identifier' @> '[{"system": "http://hl7.org/fhir/sid/us-npi"}]'
                       OR raw_json->'identifier' @> '[{"system": "http://terminology.hl7.org/NamingSystem/npi"}]') / COUNT(*), 1) AS pct_has_npi
FROM organizations
GROUP BY source_dataset ORDER BY source_dataset;

-- Identifier systems in use for organizations (what's in the non-NPI orgs?)
SELECT
    source_dataset,
    elem->>'system' AS identifier_system,
    COUNT(*)        AS occurrences
FROM organizations,
     jsonb_array_elements(raw_json->'identifier') AS elem
WHERE raw_json->'identifier' IS NOT NULL
GROUP BY source_dataset, identifier_system
ORDER BY source_dataset, occurrences DESC;

-- Sample non-NPI organization identifiers from dataset-a
SELECT raw_json->'identifier'
FROM organizations
WHERE source_dataset = 'dataset-a'
  AND NOT (raw_json->'identifier' @> '[{"system": "http://hl7.org/fhir/sid/us-npi"}]')
  AND raw_json->'identifier' IS NOT NULL
LIMIT 3;


-- ------------------------------------------------------------
-- 3. PRACTITIONER FIELD COMPLETENESS (NDH Must-Support)
-- ------------------------------------------------------------

SELECT
    source_dataset,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'name'          IS NOT NULL) / COUNT(*), 1) AS pct_name,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->>'gender'       IS NOT NULL) / COUNT(*), 1) AS pct_gender,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'qualification' IS NOT NULL) / COUNT(*), 1) AS pct_qualification,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'communication' IS NOT NULL) / COUNT(*), 1) AS pct_communication,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->>'active'       IS NOT NULL) / COUNT(*), 1) AS pct_active
FROM practitioners
GROUP BY source_dataset ORDER BY source_dataset;


-- ------------------------------------------------------------
-- 4. ORGANIZATION FIELD COMPLETENESS (NDH Must-Support)
-- ------------------------------------------------------------

SELECT
    source_dataset,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->>'active'   IS NOT NULL) / COUNT(*), 1) AS pct_active,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'type'      IS NOT NULL) / COUNT(*), 1) AS pct_type,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->>'name'     IS NOT NULL) / COUNT(*), 1) AS pct_name,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'telecom'   IS NOT NULL) / COUNT(*), 1) AS pct_telecom,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'address'   IS NOT NULL) / COUNT(*), 1) AS pct_address,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'endpoint'  IS NOT NULL) / COUNT(*), 1) AS pct_endpoint_ref
FROM organizations
GROUP BY source_dataset ORDER BY source_dataset;


-- ------------------------------------------------------------
-- 5. ORGANIZATION HIERARCHY (CMS Focus Area #2)
-- partOf is how FHIR models parent/child org relationships
-- ------------------------------------------------------------

-- How many orgs have a parent (partOf reference)?
SELECT
    source_dataset,
    COUNT(*)                                                                        AS total,
    COUNT(*) FILTER (WHERE raw_json->'partOf' IS NOT NULL)                         AS has_part_of,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'partOf' IS NOT NULL) / COUNT(*), 1) AS pct_has_part_of
FROM organizations
GROUP BY source_dataset ORDER BY source_dataset;

-- Do partOf references resolve to another org in the same dataset?
SELECT
    o.source_dataset,
    COUNT(*) FILTER (WHERE o.raw_json->'partOf' IS NOT NULL)                       AS total_with_parent,
    COUNT(*) FILTER (
        WHERE o.raw_json->'partOf' IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM organizations parent
              WHERE parent.source_dataset = o.source_dataset
                AND parent.fhir_id = split_part(o.raw_json->'partOf'->>'reference', '/', 2)
          )
    )                                                                               AS resolved,
    ROUND(100.0 * COUNT(*) FILTER (
        WHERE o.raw_json->'partOf' IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM organizations parent
              WHERE parent.source_dataset = o.source_dataset
                AND parent.fhir_id = split_part(o.raw_json->'partOf'->>'reference', '/', 2)
          )
    ) / NULLIF(COUNT(*) FILTER (WHERE o.raw_json->'partOf' IS NOT NULL), 0), 1)   AS pct_resolved
FROM organizations o
GROUP BY o.source_dataset ORDER BY o.source_dataset;

-- Hierarchy depth — how deep does the org tree go?
-- Level 1 = has parent, level 2 = parent has parent, etc.
WITH RECURSIVE org_hierarchy AS (
    SELECT fhir_id, source_dataset, raw_json->'partOf'->>'reference' AS parent_ref, 1 AS depth
    FROM organizations
    WHERE raw_json->'partOf' IS NOT NULL
    UNION ALL
    SELECT o.fhir_id, o.source_dataset, o.raw_json->'partOf'->>'reference', h.depth + 1
    FROM organizations o
    JOIN org_hierarchy h ON split_part(h.parent_ref, '/', 2) = o.fhir_id
        AND h.source_dataset = o.source_dataset
    WHERE o.raw_json->'partOf' IS NOT NULL
      AND h.depth < 10 -- safety limit
)
SELECT source_dataset, MAX(depth) AS max_hierarchy_depth, AVG(depth) AS avg_depth
FROM org_hierarchy
GROUP BY source_dataset;


-- ------------------------------------------------------------
-- 6. CORRUPTED RECORDS DETECTION (Dataset-a Organizations)
-- Find all organizations with abnormally large array fields
-- ------------------------------------------------------------

SELECT
    source_dataset,
    fhir_id,
    raw_json->>'name'                           AS name,
    jsonb_array_length(raw_json->'address')     AS address_count,
    jsonb_array_length(raw_json->'telecom')     AS telecom_count,
    jsonb_array_length(raw_json->'identifier')  AS identifier_count,
    jsonb_array_length(raw_json->'contact')     AS contact_count,
    pg_column_size(raw_json)                    AS json_size_bytes
FROM organizations
WHERE (
    (raw_json->'address'    IS NOT NULL AND jsonb_array_length(raw_json->'address')    > 100) OR
    (raw_json->'telecom'    IS NOT NULL AND jsonb_array_length(raw_json->'telecom')    > 100) OR
    (raw_json->'identifier' IS NOT NULL AND jsonb_array_length(raw_json->'identifier') > 100) OR
    (raw_json->'contact'    IS NOT NULL AND jsonb_array_length(raw_json->'contact')    > 100)
)
ORDER BY json_size_bytes DESC;


-- ------------------------------------------------------------
-- 7. PRACTITIONER → ORGANIZATION RELATIONSHIPS (CMS Focus Area #3)
-- OrganizationAffiliation is the primary resource for this
-- ------------------------------------------------------------

-- OrgAffiliation field completeness
SELECT
    source_dataset,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->>'active'                   IS NOT NULL) / COUNT(*), 1) AS pct_active,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'organization'              IS NOT NULL) / COUNT(*), 1) AS pct_organization,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'participatingOrganization' IS NOT NULL) / COUNT(*), 1) AS pct_participating_org,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'code'                      IS NOT NULL) / COUNT(*), 1) AS pct_code,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'specialty'                 IS NOT NULL) / COUNT(*), 1) AS pct_specialty,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'location'                  IS NOT NULL) / COUNT(*), 1) AS pct_location,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'endpoint'                  IS NOT NULL) / COUNT(*), 1) AS pct_endpoint
FROM organization_affiliations
GROUP BY source_dataset ORDER BY source_dataset;

-- OrgAffiliation role codes — what types of relationships are modeled?
SELECT
    source_dataset,
    cod->>'system'  AS system,
    cod->>'code'    AS code,
    cod->>'display' AS display,
    COUNT(*)        AS occurrences
FROM organization_affiliations,
     jsonb_array_elements(raw_json->'code')    AS c,
     jsonb_array_elements(c->'coding')         AS cod
WHERE raw_json->'code' IS NOT NULL
GROUP BY source_dataset, system, code, display
ORDER BY source_dataset, occurrences DESC
LIMIT 30;

-- PractitionerRole field completeness
SELECT
    source_dataset,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->>'active'       IS NOT NULL) / COUNT(*), 1) AS pct_active,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'practitioner'  IS NOT NULL) / COUNT(*), 1) AS pct_practitioner,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'organization'  IS NOT NULL) / COUNT(*), 1) AS pct_organization,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'specialty'     IS NOT NULL) / COUNT(*), 1) AS pct_specialty,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'location'      IS NOT NULL) / COUNT(*), 1) AS pct_location,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'endpoint'      IS NOT NULL) / COUNT(*), 1) AS pct_endpoint,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'availableTime' IS NOT NULL) / COUNT(*), 1) AS pct_available_time
FROM practitioner_roles
GROUP BY source_dataset ORDER BY source_dataset;


-- ------------------------------------------------------------
-- 8. NPI-BASED PRACTITIONER OVERLAP
-- More meaningful than fhir_id overlap since NPIs are stable
-- real-world identifiers independent of dataset generation
-- ------------------------------------------------------------

-- Extract NPI values and compare across datasets
WITH practitioner_npis AS (
    SELECT
        source_dataset,
        elem->>'value' AS npi
    FROM practitioners,
         jsonb_array_elements(raw_json->'identifier') AS elem
    WHERE (raw_json->'identifier' @> '[{"system": "http://hl7.org/fhir/sid/us-npi"}]'
        OR raw_json->'identifier' @> '[{"system": "http://terminology.hl7.org/NamingSystem/npi"}]')
      AND (elem->>'system' = 'http://hl7.org/fhir/sid/us-npi'
        OR elem->>'system' = 'http://terminology.hl7.org/NamingSystem/npi')
)
SELECT
    COUNT(DISTINCT npi) FILTER (WHERE source_dataset = 'dataset-a') AS dataset_a_unique_npis,
    COUNT(DISTINCT npi) FILTER (WHERE source_dataset = 'dataset-b') AS dataset_b_unique_npis,
    COUNT(DISTINCT npi) FILTER (WHERE source_dataset = 'dataset-a'
        AND npi IN (SELECT npi FROM practitioner_npis WHERE source_dataset = 'dataset-b')) AS npis_in_both,
    COUNT(DISTINCT npi) FILTER (WHERE source_dataset = 'dataset-a'
        AND npi NOT IN (SELECT npi FROM practitioner_npis WHERE source_dataset = 'dataset-b')) AS only_in_a,
    COUNT(DISTINCT npi) FILTER (WHERE source_dataset = 'dataset-b'
        AND npi NOT IN (SELECT npi FROM practitioner_npis WHERE source_dataset = 'dataset-a')) AS only_in_b
FROM practitioner_npis;


-- ------------------------------------------------------------
-- 9. DATA FRESHNESS
-- ------------------------------------------------------------

SELECT 'organizations' AS resource_type, source_dataset,
    COUNT(*) FILTER (WHERE raw_json->'meta'->>'lastUpdated' IS NOT NULL) AS has_last_updated,
    MIN(raw_json->'meta'->>'lastUpdated') AS earliest,
    MAX(raw_json->'meta'->>'lastUpdated') AS latest
FROM organizations GROUP BY source_dataset
UNION ALL
SELECT 'practitioners', source_dataset,
    COUNT(*) FILTER (WHERE raw_json->'meta'->>'lastUpdated' IS NOT NULL),
    MIN(raw_json->'meta'->>'lastUpdated'), MAX(raw_json->'meta'->>'lastUpdated')
FROM practitioners GROUP BY source_dataset
UNION ALL
SELECT 'practitioner_roles', source_dataset,
    COUNT(*) FILTER (WHERE raw_json->'meta'->>'lastUpdated' IS NOT NULL),
    MIN(raw_json->'meta'->>'lastUpdated'), MAX(raw_json->'meta'->>'lastUpdated')
FROM practitioner_roles GROUP BY source_dataset
ORDER BY resource_type, source_dataset;
