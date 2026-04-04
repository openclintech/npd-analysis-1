-- ============================================================
-- Dataset Comparison Analysis
-- NDH IG: https://build.fhir.org/ig/HL7/fhir-us-ndh/
-- Run after full ingestion to compare dataset-a vs dataset-b
-- ============================================================


-- ------------------------------------------------------------
-- 1. PIPELINE METRICS (from ingestion_runs)
-- ------------------------------------------------------------

-- Processing speed and file size by dataset
SELECT
    source_dataset,
    resource_type,
    compression,
    record_count,
    error_count,
    ROUND(compressed_size_bytes / 1024.0 / 1024.0, 1) AS compressed_size_mb,
    ROUND(download_ms / 1000.0, 1)                    AS download_sec,
    ROUND(process_ms / 1000.0, 1)                     AS process_sec,
    ROUND(total_ms / 1000.0, 1)                       AS total_sec,
    ROUND(records_per_sec::numeric, 0)                AS records_per_sec
FROM ingestion_runs
ORDER BY resource_type, source_dataset;

-- Side-by-side compression ratio comparison
SELECT
    a.resource_type,
    ROUND(a.compressed_size_bytes / 1024.0 / 1024.0, 1)                              AS dataset_a_mb,
    ROUND(b.compressed_size_bytes / 1024.0 / 1024.0, 1)                              AS dataset_b_mb,
    ROUND(a.compressed_size_bytes::numeric / NULLIF(b.compressed_size_bytes, 0), 1)  AS size_ratio_a_over_b,
    a.record_count                                                                    AS dataset_a_records,
    b.record_count                                                                    AS dataset_b_records,
    ROUND(a.process_ms / 1000.0, 1)                                                  AS dataset_a_process_sec,
    ROUND(b.process_ms / 1000.0, 1)                                                  AS dataset_b_process_sec
FROM ingestion_runs a
JOIN ingestion_runs b ON a.resource_type = b.resource_type
WHERE a.source_dataset = 'dataset-a'
  AND b.source_dataset = 'dataset-b'
ORDER BY a.resource_type;


-- ------------------------------------------------------------
-- 2. RECORD COUNTS
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
-- 3. OVERLAP ANALYSIS
-- How many fhir_ids appear in both datasets vs. unique to each?
-- ------------------------------------------------------------

SELECT
    'organizations' AS resource_type,
    COUNT(*) FILTER (WHERE source_dataset = 'dataset-a')                       AS dataset_a_total,
    COUNT(*) FILTER (WHERE source_dataset = 'dataset-b')                       AS dataset_b_total,
    COUNT(DISTINCT fhir_id) FILTER (WHERE fhir_id IN (
        SELECT fhir_id FROM organizations WHERE source_dataset = 'dataset-a'
    ) AND source_dataset = 'dataset-b')                                        AS in_both,
    COUNT(*) FILTER (WHERE source_dataset = 'dataset-a' AND fhir_id NOT IN (
        SELECT fhir_id FROM organizations WHERE source_dataset = 'dataset-b'
    ))                                                                         AS only_in_a,
    COUNT(*) FILTER (WHERE source_dataset = 'dataset-b' AND fhir_id NOT IN (
        SELECT fhir_id FROM organizations WHERE source_dataset = 'dataset-a'
    ))                                                                         AS only_in_b
FROM organizations;


-- ------------------------------------------------------------
-- 4. DATA FRESHNESS
-- Do records have lastUpdated timestamps? How recent?
-- ------------------------------------------------------------

SELECT 'organizations'           AS resource_type, source_dataset,
    COUNT(*) FILTER (WHERE raw_json->'meta'->>'lastUpdated' IS NOT NULL) AS has_last_updated,
    MIN(raw_json->'meta'->>'lastUpdated')                                AS earliest,
    MAX(raw_json->'meta'->>'lastUpdated')                                AS latest
FROM organizations GROUP BY source_dataset
UNION ALL
SELECT 'practitioners',          source_dataset,
    COUNT(*) FILTER (WHERE raw_json->'meta'->>'lastUpdated' IS NOT NULL),
    MIN(raw_json->'meta'->>'lastUpdated'), MAX(raw_json->'meta'->>'lastUpdated')
FROM practitioners GROUP BY source_dataset
UNION ALL
SELECT 'practitioner_roles',     source_dataset,
    COUNT(*) FILTER (WHERE raw_json->'meta'->>'lastUpdated' IS NOT NULL),
    MIN(raw_json->'meta'->>'lastUpdated'), MAX(raw_json->'meta'->>'lastUpdated')
FROM practitioner_roles GROUP BY source_dataset
UNION ALL
SELECT 'locations',              source_dataset,
    COUNT(*) FILTER (WHERE raw_json->'meta'->>'lastUpdated' IS NOT NULL),
    MIN(raw_json->'meta'->>'lastUpdated'), MAX(raw_json->'meta'->>'lastUpdated')
FROM locations GROUP BY source_dataset
UNION ALL
SELECT 'endpoints',              source_dataset,
    COUNT(*) FILTER (WHERE raw_json->'meta'->>'lastUpdated' IS NOT NULL),
    MIN(raw_json->'meta'->>'lastUpdated'), MAX(raw_json->'meta'->>'lastUpdated')
FROM endpoints GROUP BY source_dataset
ORDER BY resource_type, source_dataset;


-- ------------------------------------------------------------
-- 5. REFERENTIAL INTEGRITY
-- Do cross-resource references resolve within each dataset?
-- FHIR references: {"reference": "ResourceType/id"}
-- ------------------------------------------------------------

-- PractitionerRole → Practitioner
SELECT
    pr.source_dataset,
    COUNT(*)                                                                   AS total_roles,
    COUNT(*) FILTER (WHERE pr.raw_json->'practitioner' IS NULL)               AS missing_practitioner_ref,
    COUNT(*) FILTER (
        WHERE pr.raw_json->'practitioner' IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM practitioners p
              WHERE p.source_dataset = pr.source_dataset
                AND p.fhir_id = split_part(pr.raw_json->'practitioner'->>'reference', '/', 2)
          )
    )                                                                          AS unresolved_practitioner_refs,
    ROUND(100.0 * COUNT(*) FILTER (
        WHERE pr.raw_json->'practitioner' IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM practitioners p
              WHERE p.source_dataset = pr.source_dataset
                AND p.fhir_id = split_part(pr.raw_json->'practitioner'->>'reference', '/', 2)
          )
    ) / NULLIF(COUNT(*) FILTER (WHERE pr.raw_json->'practitioner' IS NOT NULL), 0), 1) AS pct_resolve
FROM practitioner_roles pr
GROUP BY pr.source_dataset ORDER BY pr.source_dataset;

-- PractitionerRole → Organization
SELECT
    pr.source_dataset,
    COUNT(*) FILTER (WHERE pr.raw_json->'organization' IS NULL)               AS missing_org_ref,
    COUNT(*) FILTER (
        WHERE pr.raw_json->'organization' IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM organizations o
              WHERE o.source_dataset = pr.source_dataset
                AND o.fhir_id = split_part(pr.raw_json->'organization'->>'reference', '/', 2)
          )
    )                                                                          AS unresolved_org_refs,
    ROUND(100.0 * COUNT(*) FILTER (
        WHERE pr.raw_json->'organization' IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM organizations o
              WHERE o.source_dataset = pr.source_dataset
                AND o.fhir_id = split_part(pr.raw_json->'organization'->>'reference', '/', 2)
          )
    ) / NULLIF(COUNT(*) FILTER (WHERE pr.raw_json->'organization' IS NOT NULL), 0), 1) AS pct_resolve
FROM practitioner_roles pr
GROUP BY pr.source_dataset ORDER BY pr.source_dataset;

-- OrganizationAffiliation → Organization
SELECT
    oa.source_dataset,
    COUNT(*) FILTER (
        WHERE oa.raw_json->'participatingOrganization' IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM organizations o
              WHERE o.source_dataset = oa.source_dataset
                AND o.fhir_id = split_part(oa.raw_json->'participatingOrganization'->>'reference', '/', 2)
          )
    )                                                                          AS unresolved_participating_org,
    COUNT(*) FILTER (
        WHERE oa.raw_json->'organization' IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM organizations o
              WHERE o.source_dataset = oa.source_dataset
                AND o.fhir_id = split_part(oa.raw_json->'organization'->>'reference', '/', 2)
          )
    )                                                                          AS unresolved_parent_org
FROM organization_affiliations oa
GROUP BY oa.source_dataset ORDER BY oa.source_dataset;

-- Location → Organization (managingOrganization)
SELECT
    l.source_dataset,
    COUNT(*) FILTER (WHERE l.raw_json->'managingOrganization' IS NULL)        AS missing_managing_org,
    COUNT(*) FILTER (
        WHERE l.raw_json->'managingOrganization' IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM organizations o
              WHERE o.source_dataset = l.source_dataset
                AND o.fhir_id = split_part(l.raw_json->'managingOrganization'->>'reference', '/', 2)
          )
    )                                                                          AS unresolved_managing_org
FROM locations l
GROUP BY l.source_dataset ORDER BY l.source_dataset;

-- Endpoint → Organization (managingOrganization)
SELECT
    e.source_dataset,
    COUNT(*) FILTER (WHERE e.raw_json->'managingOrganization' IS NULL)        AS missing_managing_org,
    COUNT(*) FILTER (
        WHERE e.raw_json->'managingOrganization' IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM organizations o
              WHERE o.source_dataset = e.source_dataset
                AND o.fhir_id = split_part(e.raw_json->'managingOrganization'->>'reference', '/', 2)
          )
    )                                                                          AS unresolved_managing_org
FROM endpoints e
GROUP BY e.source_dataset ORDER BY e.source_dataset;


-- ------------------------------------------------------------
-- 6. NDH MUST-SUPPORT: IDENTIFIERS
-- NPI is a critical must-support identifier in the NDH IG.
-- NPI system: http://hl7.org/fhir/sid/us-npi
-- ------------------------------------------------------------

-- Practitioner NPI coverage
SELECT
    source_dataset,
    COUNT(*)                                                                   AS total,
    COUNT(*) FILTER (WHERE raw_json->'identifier' IS NOT NULL)                AS has_any_identifier,
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'identifier') id
        WHERE id->>'system' = 'http://hl7.org/fhir/sid/us-npi'
    ))                                                                         AS has_npi,
    ROUND(100.0 * COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'identifier') id
        WHERE id->>'system' = 'http://hl7.org/fhir/sid/us-npi'
    )) / COUNT(*), 1)                                                          AS pct_has_npi
FROM practitioners
GROUP BY source_dataset ORDER BY source_dataset;

-- Organization NPI coverage
SELECT
    source_dataset,
    COUNT(*)                                                                   AS total,
    COUNT(*) FILTER (WHERE raw_json->'identifier' IS NOT NULL)                AS has_any_identifier,
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'identifier') id
        WHERE id->>'system' = 'http://hl7.org/fhir/sid/us-npi'
    ))                                                                         AS has_npi,
    ROUND(100.0 * COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'identifier') id
        WHERE id->>'system' = 'http://hl7.org/fhir/sid/us-npi'
    )) / COUNT(*), 1)                                                          AS pct_has_npi
FROM organizations
GROUP BY source_dataset ORDER BY source_dataset;

-- Identifier systems in use across practitioners (what systems beyond NPI?)
SELECT
    source_dataset,
    id->>'system'  AS identifier_system,
    COUNT(*)       AS occurrences
FROM practitioners,
     jsonb_array_elements(raw_json->'identifier') AS id
WHERE raw_json->'identifier' IS NOT NULL
GROUP BY source_dataset, identifier_system
ORDER BY source_dataset, occurrences DESC;


-- ------------------------------------------------------------
-- 7. NDH MUST-SUPPORT: FIELD COMPLETENESS PER RESOURCE
-- Based on NDH IG must-support elements per profile
-- ------------------------------------------------------------

-- NdhPractitioner must-support fields
SELECT
    source_dataset,
    COUNT(*)                                                                                   AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'identifier'   IS NOT NULL) / COUNT(*), 1) AS pct_identifier,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'name'         IS NOT NULL) / COUNT(*), 1) AS pct_name,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->>'gender'      IS NOT NULL) / COUNT(*), 1) AS pct_gender,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'qualification' IS NOT NULL) / COUNT(*), 1) AS pct_qualification,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'communication' IS NOT NULL) / COUNT(*), 1) AS pct_communication,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->>'active'      IS NOT NULL) / COUNT(*), 1) AS pct_active,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'photo'        IS NOT NULL) / COUNT(*), 1) AS pct_photo
FROM practitioners
GROUP BY source_dataset ORDER BY source_dataset;

-- NdhOrganization must-support fields
SELECT
    source_dataset,
    COUNT(*)                                                                                   AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'identifier'   IS NOT NULL) / COUNT(*), 1) AS pct_identifier,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->>'active'      IS NOT NULL) / COUNT(*), 1) AS pct_active,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'type'         IS NOT NULL) / COUNT(*), 1) AS pct_type,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->>'name'        IS NOT NULL) / COUNT(*), 1) AS pct_name,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'alias'        IS NOT NULL) / COUNT(*), 1) AS pct_alias,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'telecom'      IS NOT NULL) / COUNT(*), 1) AS pct_telecom,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'address'      IS NOT NULL) / COUNT(*), 1) AS pct_address,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'contact'      IS NOT NULL) / COUNT(*), 1) AS pct_contact,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'endpoint'     IS NOT NULL) / COUNT(*), 1) AS pct_endpoint_ref
FROM organizations
GROUP BY source_dataset ORDER BY source_dataset;

-- NdhPractitionerRole must-support fields
SELECT
    source_dataset,
    COUNT(*)                                                                                   AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->>'active'        IS NOT NULL) / COUNT(*), 1) AS pct_active,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'practitioner'   IS NOT NULL) / COUNT(*), 1) AS pct_practitioner,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'organization'   IS NOT NULL) / COUNT(*), 1) AS pct_organization,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'code'           IS NOT NULL) / COUNT(*), 1) AS pct_code,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'specialty'      IS NOT NULL) / COUNT(*), 1) AS pct_specialty,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'location'       IS NOT NULL) / COUNT(*), 1) AS pct_location,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'endpoint'       IS NOT NULL) / COUNT(*), 1) AS pct_endpoint,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'availableTime'  IS NOT NULL) / COUNT(*), 1) AS pct_available_time,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'notAvailable'   IS NOT NULL) / COUNT(*), 1) AS pct_not_available
FROM practitioner_roles
GROUP BY source_dataset ORDER BY source_dataset;

-- NdhOrganizationAffiliation must-support fields
SELECT
    source_dataset,
    COUNT(*)                                                                                          AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->>'active'                   IS NOT NULL) / COUNT(*), 1) AS pct_active,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'organization'              IS NOT NULL) / COUNT(*), 1) AS pct_organization,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'participatingOrganization' IS NOT NULL) / COUNT(*), 1) AS pct_participating_org,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'code'                      IS NOT NULL) / COUNT(*), 1) AS pct_code,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'specialty'                 IS NOT NULL) / COUNT(*), 1) AS pct_specialty,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'location'                  IS NOT NULL) / COUNT(*), 1) AS pct_location,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'endpoint'                  IS NOT NULL) / COUNT(*), 1) AS pct_endpoint
FROM organization_affiliations
GROUP BY source_dataset ORDER BY source_dataset;

-- NdhLocation must-support fields
SELECT
    source_dataset,
    COUNT(*)                                                                                   AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->>'status'            IS NOT NULL) / COUNT(*), 1) AS pct_status,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->>'name'              IS NOT NULL) / COUNT(*), 1) AS pct_name,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'type'               IS NOT NULL) / COUNT(*), 1) AS pct_type,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'telecom'            IS NOT NULL) / COUNT(*), 1) AS pct_telecom,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'address'            IS NOT NULL) / COUNT(*), 1) AS pct_address,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'position'           IS NOT NULL) / COUNT(*), 1) AS pct_position,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'managingOrganization' IS NOT NULL) / COUNT(*), 1) AS pct_managing_org,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'hoursOfOperation'   IS NOT NULL) / COUNT(*), 1) AS pct_hours_of_operation
FROM locations
GROUP BY source_dataset ORDER BY source_dataset;

-- NdhEndpoint must-support fields
SELECT
    source_dataset,
    COUNT(*)                                                                                   AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->>'status'            IS NOT NULL) / COUNT(*), 1) AS pct_status,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'connectionType'     IS NOT NULL) / COUNT(*), 1) AS pct_connection_type,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->>'name'              IS NOT NULL) / COUNT(*), 1) AS pct_name,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'managingOrganization' IS NOT NULL) / COUNT(*), 1) AS pct_managing_org,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'payloadType'        IS NOT NULL) / COUNT(*), 1) AS pct_payload_type,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->>'address'           IS NOT NULL) / COUNT(*), 1) AS pct_address
FROM endpoints
GROUP BY source_dataset ORDER BY source_dataset;


-- ------------------------------------------------------------
-- 8. NDH MUST-SUPPORT: EXTENSIONS
-- NDH defines several important extensions. Presence of these
-- indicates richer, more complete data for directory use cases.
-- ------------------------------------------------------------

-- New patient acceptance (PractitionerRole) — critical for directory usability
SELECT
    source_dataset,
    COUNT(*)                                                                   AS total,
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'extension') ext
        WHERE ext->>'url' LIKE '%newpatients%'
    ))                                                                         AS has_new_patients_ext,
    ROUND(100.0 * COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'extension') ext
        WHERE ext->>'url' LIKE '%newpatients%'
    )) / COUNT(*), 1)                                                          AS pct_has_new_patients
FROM practitioner_roles
WHERE raw_json->'extension' IS NOT NULL
GROUP BY source_dataset ORDER BY source_dataset;

-- Network participation (PractitionerRole)
SELECT
    source_dataset,
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'extension') ext
        WHERE ext->>'url' LIKE '%network%'
    ))                                                                         AS has_network_ext,
    ROUND(100.0 * COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'extension') ext
        WHERE ext->>'url' LIKE '%network%'
    )) / NULLIF(COUNT(*) FILTER (WHERE raw_json->'extension' IS NOT NULL), 0), 1) AS pct_has_network
FROM practitioner_roles
GROUP BY source_dataset ORDER BY source_dataset;

-- Qualification status extension (Practitioner)
SELECT
    source_dataset,
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'extension') ext
        WHERE ext->>'url' LIKE '%qualification%'
    ))                                                                         AS has_qualification_ext,
    ROUND(100.0 * COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'extension') ext
        WHERE ext->>'url' LIKE '%qualification%'
    )) / NULLIF(COUNT(*) FILTER (WHERE raw_json->'extension' IS NOT NULL), 0), 1) AS pct_has_qualification_ext
FROM practitioners
GROUP BY source_dataset ORDER BY source_dataset;

-- Accessibility extension (Location)
SELECT
    source_dataset,
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'extension') ext
        WHERE ext->>'url' LIKE '%accessibility%'
    ))                                                                         AS has_accessibility_ext,
    ROUND(100.0 * COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'extension') ext
        WHERE ext->>'url' LIKE '%accessibility%'
    )) / NULLIF(COUNT(*) FILTER (WHERE raw_json->'extension' IS NOT NULL), 0), 1) AS pct_has_accessibility
FROM locations
GROUP BY source_dataset ORDER BY source_dataset;

-- All extension URLs in use — useful for discovering what each dataset actually populates
SELECT
    source_dataset,
    ext->>'url'  AS extension_url,
    COUNT(*)     AS occurrences
FROM practitioners,
     jsonb_array_elements(raw_json->'extension') AS ext
WHERE raw_json->'extension' IS NOT NULL
GROUP BY source_dataset, extension_url
ORDER BY source_dataset, occurrences DESC
LIMIT 50;


-- ------------------------------------------------------------
-- 9. NDH MUST-SUPPORT: SPECIALTY CODING (NUCC Taxonomy)
-- NDH requires specialty coded with NUCC taxonomy where applicable.
-- NUCC system: http://nucc.org/provider-taxonomy
-- ------------------------------------------------------------

-- PractitionerRole specialty — NUCC coverage
SELECT
    source_dataset,
    COUNT(*)                                                                   AS total_with_specialty,
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'specialty') spec,
                      jsonb_array_elements(spec->'coding') cod
        WHERE cod->>'system' = 'http://nucc.org/provider-taxonomy'
    ))                                                                         AS has_nucc_specialty,
    ROUND(100.0 * COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'specialty') spec,
                      jsonb_array_elements(spec->'coding') cod
        WHERE cod->>'system' = 'http://nucc.org/provider-taxonomy'
    )) / COUNT(*), 1)                                                          AS pct_nucc_coded
FROM practitioner_roles
WHERE raw_json->'specialty' IS NOT NULL
GROUP BY source_dataset ORDER BY source_dataset;

-- Top specialties in each dataset
SELECT
    source_dataset,
    cod->>'system'  AS system,
    cod->>'code'    AS code,
    cod->>'display' AS display,
    COUNT(*)        AS occurrences
FROM practitioner_roles,
     jsonb_array_elements(raw_json->'specialty') AS spec,
     jsonb_array_elements(spec->'coding')        AS cod
WHERE raw_json->'specialty' IS NOT NULL
GROUP BY source_dataset, system, code, display
ORDER BY source_dataset, occurrences DESC
LIMIT 30;

-- Organization type code systems
SELECT
    source_dataset,
    cod->>'system'  AS system,
    cod->>'code'    AS code,
    cod->>'display' AS display,
    COUNT(*)        AS occurrences
FROM organizations,
     jsonb_array_elements(raw_json->'type')   AS typ,
     jsonb_array_elements(typ->'coding')      AS cod
WHERE raw_json->'type' IS NOT NULL
GROUP BY source_dataset, system, code, display
ORDER BY source_dataset, occurrences DESC
LIMIT 30;


-- ------------------------------------------------------------
-- 10. NDH MUST-SUPPORT: ENDPOINT DETAILS
-- Endpoints are critical for directory interoperability.
-- ------------------------------------------------------------

-- Connection type coverage and breakdown
SELECT
    source_dataset,
    raw_json->'connectionType'->>'system'  AS connection_type_system,
    raw_json->'connectionType'->>'code'    AS connection_type_code,
    COUNT(*)                               AS occurrences
FROM endpoints
WHERE raw_json->'connectionType' IS NOT NULL
GROUP BY source_dataset, connection_type_system, connection_type_code
ORDER BY source_dataset, occurrences DESC;

-- Payload type coverage
SELECT
    source_dataset,
    COUNT(*)                                                                   AS total,
    COUNT(*) FILTER (WHERE raw_json->'payloadType' IS NOT NULL)               AS has_payload_type,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'payloadType' IS NOT NULL) / COUNT(*), 1) AS pct_payload_type,
    COUNT(*) FILTER (WHERE raw_json->>'address' IS NOT NULL)                  AS has_address,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->>'address' IS NOT NULL) / COUNT(*), 1)    AS pct_address
FROM endpoints
GROUP BY source_dataset ORDER BY source_dataset;

-- Endpoint status breakdown
SELECT
    source_dataset,
    raw_json->>'status' AS status,
    COUNT(*)            AS occurrences
FROM endpoints
GROUP BY source_dataset, status
ORDER BY source_dataset, occurrences DESC;
