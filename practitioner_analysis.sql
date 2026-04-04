-- ============================================================
-- Practitioner + PractitionerRole Analysis
-- NDH IG profiles:
--   https://build.fhir.org/ig/HL7/fhir-us-ndh/StructureDefinition-ndh-Practitioner.html
--   https://build.fhir.org/ig/HL7/fhir-us-ndh/StructureDefinition-ndh-PractitionerRole.html
--
-- Key NDH requirements:
--   Practitioner: identifier (NPI, 1..*), active (1..1), name (1..*), qualification
--   PractitionerRole: active (1..1), practitioner OR org/location/service,
--                     telecom OR endpoint (contact requirement)
-- ============================================================


-- ============================================================
-- SECTION 1: PRACTITIONER FIELD COMPLETENESS (NDH Must-Support)
-- ============================================================

-- Core fields
SELECT
    source_dataset,
    COUNT(*)                                                                                      AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->>'active'       IS NOT NULL) / COUNT(*), 1)  AS pct_active,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'name'          IS NOT NULL) / COUNT(*), 1)  AS pct_name,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->>'gender'       IS NOT NULL) / COUNT(*), 1)  AS pct_gender,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'identifier'    IS NOT NULL) / COUNT(*), 1)  AS pct_any_identifier,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'telecom'       IS NOT NULL) / COUNT(*), 1)  AS pct_telecom,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'address'       IS NOT NULL) / COUNT(*), 1)  AS pct_address,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'qualification' IS NOT NULL) / COUNT(*), 1)  AS pct_qualification,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'communication' IS NOT NULL) / COUNT(*), 1)  AS pct_communication
FROM practitioners
GROUP BY source_dataset ORDER BY source_dataset;

-- NDH extensions on Practitioner
-- endpoint-reference: links practitioner directly to an endpoint
-- qualification extension (base-ext-qualification): extends qualification element
-- verification-status: attestation/data quality tracking
-- communication-proficiency: language proficiency levels
SELECT
    source_dataset,
    COUNT(*)                                                                                      AS total,
    COUNT(*) FILTER (WHERE raw_json->'extension' IS NOT NULL)                                   AS has_any_extension,
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'extension') e
        WHERE e->>'url' LIKE '%endpoint%'
    ))                                                                                            AS has_endpoint_ref_ext,
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'extension') e
        WHERE e->>'url' LIKE '%verification-status%'
    ))                                                                                            AS has_verification_status,
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'extension') e
        WHERE e->>'url' LIKE '%communication-proficiency%'
    ))                                                                                            AS has_comm_proficiency,
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'extension') e
        WHERE e->>'url' LIKE '%rating%'
    ))                                                                                            AS has_rating,
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'extension') e
        WHERE e->>'url' LIKE '%accessibility%'
    ))                                                                                            AS has_accessibility
FROM practitioners
GROUP BY source_dataset ORDER BY source_dataset;

-- All extension URLs on Practitioner — discover what each dataset actually populates
SELECT
    source_dataset,
    e->>'url'   AS extension_url,
    COUNT(*)    AS occurrences
FROM practitioners,
     jsonb_array_elements(raw_json->'extension') AS e
WHERE raw_json->'extension' IS NOT NULL
GROUP BY source_dataset, extension_url
ORDER BY source_dataset, occurrences DESC;


-- ============================================================
-- SECTION 2: PRACTITIONER QUALIFICATION DEPTH
-- NDH requires qualification with code; issuer org reference encouraged
-- NUCC taxonomy: http://nucc.org/provider-taxonomy
-- ============================================================

-- Qualification field presence
SELECT
    source_dataset,
    COUNT(*)                                                                                     AS total,
    COUNT(*) FILTER (WHERE raw_json->'qualification' IS NOT NULL)                               AS has_qualification,
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'qualification') q
        WHERE q->'code' IS NOT NULL
    ))                                                                                           AS has_qual_code,
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'qualification') q
        WHERE q->'issuer' IS NOT NULL
    ))                                                                                           AS has_qual_issuer,
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'qualification') q
        WHERE q->'period' IS NOT NULL
    ))                                                                                           AS has_qual_period,
    -- qualification extension (NDH adds status, whereValid to qualification element)
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'qualification') q,
                      jsonb_array_elements(q->'extension') e
        WHERE e->>'url' LIKE '%qualification%'
    ))                                                                                           AS has_qual_extension
FROM practitioners
GROUP BY source_dataset ORDER BY source_dataset;

-- Qualification code systems in use
SELECT
    source_dataset,
    cod->>'system'   AS code_system,
    COUNT(*)         AS occurrences
FROM practitioners,
     jsonb_array_elements(raw_json->'qualification') AS q,
     jsonb_array_elements(q->'code'->'coding')       AS cod
WHERE raw_json->'qualification' IS NOT NULL
  AND q->'code' IS NOT NULL
GROUP BY source_dataset, code_system
ORDER BY source_dataset, occurrences DESC;

-- Top qualification codes (what credentials/licenses are captured?)
SELECT
    source_dataset,
    cod->>'system'   AS system,
    cod->>'code'     AS code,
    cod->>'display'  AS display,
    COUNT(*)         AS occurrences
FROM practitioners,
     jsonb_array_elements(raw_json->'qualification') AS q,
     jsonb_array_elements(q->'code'->'coding')       AS cod
WHERE raw_json->'qualification' IS NOT NULL
  AND q->'code' IS NOT NULL
GROUP BY source_dataset, system, code, display
ORDER BY source_dataset, occurrences DESC
LIMIT 40;

-- Avg qualifications per practitioner (how many credentials per person?)
SELECT
    source_dataset,
    ROUND(AVG(jsonb_array_length(raw_json->'qualification')), 2) AS avg_qualifications_per_practitioner,
    MAX(jsonb_array_length(raw_json->'qualification'))           AS max_qualifications,
    MIN(jsonb_array_length(raw_json->'qualification'))           AS min_qualifications
FROM practitioners
WHERE raw_json->'qualification' IS NOT NULL
GROUP BY source_dataset ORDER BY source_dataset;


-- ============================================================
-- SECTION 3: PRACTITIONERROLE FIELD COMPLETENESS (NDH Must-Support)
-- ============================================================

-- Core fields
SELECT
    source_dataset,
    COUNT(*)                                                                                      AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->>'active'        IS NOT NULL) / COUNT(*), 1) AS pct_active,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'practitioner'   IS NOT NULL) / COUNT(*), 1) AS pct_practitioner,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'organization'   IS NOT NULL) / COUNT(*), 1) AS pct_organization,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'code'           IS NOT NULL) / COUNT(*), 1) AS pct_role_code,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'specialty'      IS NOT NULL) / COUNT(*), 1) AS pct_specialty,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'location'       IS NOT NULL) / COUNT(*), 1) AS pct_location,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'telecom'        IS NOT NULL) / COUNT(*), 1) AS pct_telecom,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'endpoint'       IS NOT NULL) / COUNT(*), 1) AS pct_endpoint,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'availableTime'  IS NOT NULL) / COUNT(*), 1) AS pct_available_time,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'notAvailable'   IS NOT NULL) / COUNT(*), 1) AS pct_not_available
FROM practitioner_roles
GROUP BY source_dataset ORDER BY source_dataset;

-- NDH contact requirement: PractitionerRole SHALL have telecom OR endpoint
-- Roles with neither are non-compliant with NDH invariant
SELECT
    source_dataset,
    COUNT(*)                                                                                       AS total,
    COUNT(*) FILTER (WHERE raw_json->'telecom'  IS NOT NULL)                                     AS has_telecom,
    COUNT(*) FILTER (WHERE raw_json->'endpoint' IS NOT NULL)                                     AS has_endpoint,
    COUNT(*) FILTER (WHERE raw_json->'telecom'  IS NOT NULL OR  raw_json->'endpoint' IS NOT NULL) AS has_telecom_or_endpoint,
    COUNT(*) FILTER (WHERE raw_json->'telecom'  IS NULL     AND raw_json->'endpoint' IS NULL)    AS missing_both,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'telecom' IS NULL AND raw_json->'endpoint' IS NULL) / COUNT(*), 1) AS pct_missing_both
FROM practitioner_roles
GROUP BY source_dataset ORDER BY source_dataset;

-- NDH extensions on PractitionerRole
SELECT
    source_dataset,
    COUNT(*)                                                                                      AS total,
    COUNT(*) FILTER (WHERE raw_json->'extension' IS NOT NULL)                                   AS has_any_extension,
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'extension') e
        WHERE e->>'url' LIKE '%newpatients%'
    ))                                                                                            AS has_new_patients,
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'extension') e
        WHERE e->>'url' LIKE '%network%'
    ))                                                                                            AS has_network,
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'extension') e
        WHERE e->>'url' LIKE '%verification-status%'
    ))                                                                                            AS has_verification_status,
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'extension') e
        WHERE e->>'url' LIKE '%qualification%'
    ))                                                                                            AS has_qualification_ext,
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'extension') e
        WHERE e->>'url' LIKE '%rating%'
    ))                                                                                            AS has_rating
FROM practitioner_roles
GROUP BY source_dataset ORDER BY source_dataset;

-- All extension URLs on PractitionerRole
SELECT
    source_dataset,
    e->>'url'   AS extension_url,
    COUNT(*)    AS occurrences
FROM practitioner_roles,
     jsonb_array_elements(raw_json->'extension') AS e
WHERE raw_json->'extension' IS NOT NULL
GROUP BY source_dataset, extension_url
ORDER BY source_dataset, occurrences DESC;

-- PractitionerRole role codes (what types of roles are modeled?)
SELECT
    source_dataset,
    cod->>'system'   AS system,
    cod->>'code'     AS code,
    cod->>'display'  AS display,
    COUNT(*)         AS occurrences
FROM practitioner_roles,
     jsonb_array_elements(raw_json->'code')    AS c,
     jsonb_array_elements(c->'coding')         AS cod
WHERE raw_json->'code' IS NOT NULL
GROUP BY source_dataset, system, code, display
ORDER BY source_dataset, occurrences DESC
LIMIT 30;


-- ============================================================
-- SECTION 4: SPECIALTY CODING (NUCC Taxonomy)
-- NDH binds PractitionerRole.specialty to NUCC taxonomy
-- System: http://nucc.org/provider-taxonomy
-- ============================================================

-- NUCC coverage on PractitionerRole
SELECT
    source_dataset,
    COUNT(*)                                                                                     AS total,
    COUNT(*) FILTER (WHERE raw_json->'specialty' IS NOT NULL)                                  AS has_specialty,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'specialty' IS NOT NULL) / COUNT(*), 1)    AS pct_has_specialty,
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'specialty') spec,
                      jsonb_array_elements(spec->'coding') cod
        WHERE cod->>'system' = 'http://nucc.org/provider-taxonomy'
    ))                                                                                          AS has_nucc_specialty,
    ROUND(100.0 * COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(raw_json->'specialty') spec,
                      jsonb_array_elements(spec->'coding') cod
        WHERE cod->>'system' = 'http://nucc.org/provider-taxonomy'
    )) / COUNT(*), 1)                                                                           AS pct_nucc_specialty
FROM practitioner_roles
GROUP BY source_dataset ORDER BY source_dataset;

-- Top 20 specialties per dataset
SELECT
    source_dataset,
    cod->>'system'   AS system,
    cod->>'code'     AS code,
    cod->>'display'  AS display,
    COUNT(*)         AS occurrences
FROM practitioner_roles,
     jsonb_array_elements(raw_json->'specialty') AS spec,
     jsonb_array_elements(spec->'coding')        AS cod
WHERE raw_json->'specialty' IS NOT NULL
GROUP BY source_dataset, system, code, display
ORDER BY source_dataset, occurrences DESC
LIMIT 40;

-- Avg specialties per role
SELECT
    source_dataset,
    ROUND(AVG(jsonb_array_length(raw_json->'specialty')), 2) AS avg_specialties_per_role,
    MAX(jsonb_array_length(raw_json->'specialty'))           AS max_specialties,
    MIN(jsonb_array_length(raw_json->'specialty'))           AS min_specialties
FROM practitioner_roles
WHERE raw_json->'specialty' IS NOT NULL
GROUP BY source_dataset ORDER BY source_dataset;


-- ============================================================
-- SECTION 5: REFERENTIAL INTEGRITY
-- Within each dataset: do PractitionerRole references resolve?
-- Dataset-b may use absolute URLs: http://dev.cnpd.internal.cms.gov/fhir/Practitioner/{id}
-- Split on last '/' to handle both relative and absolute references.
-- ============================================================

-- PractitionerRole → Practitioner resolution rate
SELECT
    pr.source_dataset,
    COUNT(*)                                                                                     AS total_roles,
    COUNT(*) FILTER (WHERE pr.raw_json->'practitioner' IS NULL)                                AS missing_practitioner_ref,
    COUNT(*) FILTER (WHERE pr.raw_json->'practitioner' IS NOT NULL)                            AS has_practitioner_ref,
    COUNT(*) FILTER (
        WHERE pr.raw_json->'practitioner' IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM practitioners p
              WHERE p.source_dataset = pr.source_dataset
                AND p.fhir_id = reverse(split_part(reverse(pr.raw_json->'practitioner'->>'reference'), '/', 1))
          )
    )                                                                                           AS resolved,
    ROUND(100.0 * COUNT(*) FILTER (
        WHERE pr.raw_json->'practitioner' IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM practitioners p
              WHERE p.source_dataset = pr.source_dataset
                AND p.fhir_id = reverse(split_part(reverse(pr.raw_json->'practitioner'->>'reference'), '/', 1))
          )
    ) / NULLIF(COUNT(*) FILTER (WHERE pr.raw_json->'practitioner' IS NOT NULL), 0), 1)        AS pct_resolved
FROM practitioner_roles pr
GROUP BY pr.source_dataset ORDER BY pr.source_dataset;

-- PractitionerRole → Organization resolution rate
SELECT
    pr.source_dataset,
    COUNT(*)                                                                                     AS total_roles,
    COUNT(*) FILTER (WHERE pr.raw_json->'organization' IS NULL)                                AS missing_org_ref,
    COUNT(*) FILTER (WHERE pr.raw_json->'organization' IS NOT NULL)                            AS has_org_ref,
    COUNT(*) FILTER (
        WHERE pr.raw_json->'organization' IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM organizations o
              WHERE o.source_dataset = pr.source_dataset
                AND o.fhir_id = reverse(split_part(reverse(pr.raw_json->'organization'->>'reference'), '/', 1))
          )
    )                                                                                           AS resolved,
    ROUND(100.0 * COUNT(*) FILTER (
        WHERE pr.raw_json->'organization' IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM organizations o
              WHERE o.source_dataset = pr.source_dataset
                AND o.fhir_id = reverse(split_part(reverse(pr.raw_json->'organization'->>'reference'), '/', 1))
          )
    ) / NULLIF(COUNT(*) FILTER (WHERE pr.raw_json->'organization' IS NOT NULL), 0), 1)        AS pct_resolved
FROM practitioner_roles pr
GROUP BY pr.source_dataset ORDER BY pr.source_dataset;

-- PractitionerRole → Location resolution rate
SELECT
    pr.source_dataset,
    COUNT(*)                                                                                     AS total_roles,
    COUNT(*) FILTER (WHERE raw_json->'location' IS NULL)                                       AS missing_location_ref,
    COUNT(*) FILTER (WHERE raw_json->'location' IS NOT NULL)                                   AS has_location_ref,
    COUNT(*) FILTER (
        WHERE pr.raw_json->'location' IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM locations l
              WHERE l.source_dataset = pr.source_dataset
                AND l.fhir_id = reverse(split_part(reverse(
                    (jsonb_array_elements(pr.raw_json->'location')->>'reference')
                ), '/', 1))
          )
    )                                                                                           AS resolved_count
FROM practitioner_roles pr
GROUP BY pr.source_dataset ORDER BY pr.source_dataset;


-- ============================================================
-- SECTION 6: PRACTITIONER COVERAGE BY PRACTITIONERROLE
-- What % of practitioners have at least one PractitionerRole?
-- Uses NPI as the join key — stable real-world identifier,
-- independent of how each dataset generates FHIR IDs.
-- ============================================================

-- Extract NPIs from practitioners (handles both system URLs)
-- dataset-a: http://hl7.org/fhir/sid/us-npi
-- dataset-b: http://terminology.hl7.org/NamingSystem/npi
WITH practitioner_npi AS (
    SELECT
        p.source_dataset,
        p.fhir_id,
        elem->>'value' AS npi
    FROM practitioners p,
         jsonb_array_elements(p.raw_json->'identifier') AS elem
    WHERE (elem->>'system' = 'http://hl7.org/fhir/sid/us-npi'
        OR elem->>'system' = 'http://terminology.hl7.org/NamingSystem/npi')
),
-- Extract NPIs referenced from PractitionerRole → Practitioner (via fhir_id join within dataset)
role_practitioner AS (
    SELECT
        pr.source_dataset,
        reverse(split_part(reverse(pr.raw_json->'practitioner'->>'reference'), '/', 1)) AS practitioner_fhir_id
    FROM practitioner_roles pr
    WHERE pr.raw_json->'practitioner' IS NOT NULL
)
SELECT
    pn.source_dataset,
    COUNT(DISTINCT pn.fhir_id)                                                         AS total_practitioners,
    COUNT(DISTINCT rp.practitioner_fhir_id)                                            AS practitioners_with_role,
    ROUND(100.0 * COUNT(DISTINCT rp.practitioner_fhir_id) /
        NULLIF(COUNT(DISTINCT pn.fhir_id), 0), 1)                                     AS pct_with_role,
    COUNT(DISTINCT pn.fhir_id) - COUNT(DISTINCT rp.practitioner_fhir_id)              AS practitioners_without_role
FROM practitioner_npi pn
LEFT JOIN role_practitioner rp
    ON rp.source_dataset = pn.source_dataset
   AND rp.practitioner_fhir_id = pn.fhir_id
GROUP BY pn.source_dataset ORDER BY pn.source_dataset;


-- ============================================================
-- SECTION 7: DIRECTORY COMPLETENESS (JOINED VIEW)
-- A "directory-complete" practitioner entry requires:
--   - Practitioner: NPI + name + active
--   - PractitionerRole: specialty + (location OR organization) + (telecom OR endpoint)
-- ============================================================

WITH practitioner_base AS (
    SELECT
        p.source_dataset,
        p.fhir_id,
        -- NPI
        (p.raw_json->'identifier' @> '[{"system": "http://hl7.org/fhir/sid/us-npi"}]'
            OR p.raw_json->'identifier' @> '[{"system": "http://terminology.hl7.org/NamingSystem/npi"}]'
        )                                                             AS has_npi,
        -- Required NDH Practitioner fields
        (p.raw_json->>'active' IS NOT NULL)                          AS has_active,
        (p.raw_json->'name' IS NOT NULL)                             AS has_name,
        (p.raw_json->>'gender' IS NOT NULL)                          AS has_gender,
        (p.raw_json->'qualification' IS NOT NULL)                    AS has_qualification
    FROM practitioners p
),
role_summary AS (
    SELECT
        pr.source_dataset,
        reverse(split_part(reverse(pr.raw_json->'practitioner'->>'reference'), '/', 1)) AS practitioner_fhir_id,
        COUNT(*)                                                      AS role_count,
        -- Has specialty (NUCC preferred)
        bool_or(pr.raw_json->'specialty' IS NOT NULL)                AS any_specialty,
        bool_or(EXISTS (
            SELECT 1 FROM jsonb_array_elements(pr.raw_json->'specialty') spec,
                          jsonb_array_elements(spec->'coding') cod
            WHERE cod->>'system' = 'http://nucc.org/provider-taxonomy'
        ))                                                            AS any_nucc_specialty,
        -- Has location or organization reference
        bool_or(pr.raw_json->'location' IS NOT NULL
            OR pr.raw_json->'organization' IS NOT NULL)              AS any_location_or_org,
        -- Has contact info (telecom or endpoint)
        bool_or(pr.raw_json->'telecom' IS NOT NULL
            OR pr.raw_json->'endpoint' IS NOT NULL)                  AS any_contact,
        -- Has availability info
        bool_or(pr.raw_json->'availableTime' IS NOT NULL)            AS any_available_time,
        -- Has NDH extensions
        bool_or(EXISTS (
            SELECT 1 FROM jsonb_array_elements(pr.raw_json->'extension') e
            WHERE e->>'url' LIKE '%newpatients%'
        ))                                                            AS any_new_patients_ext,
        bool_or(EXISTS (
            SELECT 1 FROM jsonb_array_elements(pr.raw_json->'extension') e
            WHERE e->>'url' LIKE '%network%'
        ))                                                            AS any_network_ext
    FROM practitioner_roles pr
    WHERE pr.raw_json->'practitioner' IS NOT NULL
    GROUP BY pr.source_dataset, practitioner_fhir_id
)
SELECT
    pb.source_dataset,
    COUNT(*)                                                                                   AS total_practitioners,
    -- Core identity completeness
    ROUND(100.0 * COUNT(*) FILTER (WHERE pb.has_npi)                           / COUNT(*), 1) AS pct_npi,
    ROUND(100.0 * COUNT(*) FILTER (WHERE pb.has_active)                        / COUNT(*), 1) AS pct_active,
    ROUND(100.0 * COUNT(*) FILTER (WHERE pb.has_name)                          / COUNT(*), 1) AS pct_name,
    ROUND(100.0 * COUNT(*) FILTER (WHERE pb.has_gender)                        / COUNT(*), 1) AS pct_gender,
    ROUND(100.0 * COUNT(*) FILTER (WHERE pb.has_qualification)                 / COUNT(*), 1) AS pct_qualification,
    -- Role linkage
    ROUND(100.0 * COUNT(*) FILTER (WHERE rs.practitioner_fhir_id IS NOT NULL)  / COUNT(*), 1) AS pct_with_any_role,
    -- Combined quality (have role with key directory fields)
    ROUND(100.0 * COUNT(*) FILTER (WHERE rs.any_specialty)                     / COUNT(*), 1) AS pct_with_specialty_via_role,
    ROUND(100.0 * COUNT(*) FILTER (WHERE rs.any_nucc_specialty)                / COUNT(*), 1) AS pct_with_nucc_specialty,
    ROUND(100.0 * COUNT(*) FILTER (WHERE rs.any_location_or_org)               / COUNT(*), 1) AS pct_with_location_or_org,
    ROUND(100.0 * COUNT(*) FILTER (WHERE rs.any_contact)                       / COUNT(*), 1) AS pct_with_contact,
    ROUND(100.0 * COUNT(*) FILTER (WHERE rs.any_available_time)                / COUNT(*), 1) AS pct_with_availability,
    ROUND(100.0 * COUNT(*) FILTER (WHERE rs.any_new_patients_ext)              / COUNT(*), 1) AS pct_with_new_patients,
    ROUND(100.0 * COUNT(*) FILTER (WHERE rs.any_network_ext)                   / COUNT(*), 1) AS pct_with_network,
    -- Fully directory-complete: NPI + name + active + role with NUCC specialty + location/org + contact
    ROUND(100.0 * COUNT(*) FILTER (
        WHERE pb.has_npi
          AND pb.has_name
          AND pb.has_active
          AND rs.any_nucc_specialty
          AND rs.any_location_or_org
          AND rs.any_contact
    ) / COUNT(*), 1)                                                                           AS pct_directory_complete
FROM practitioner_base pb
LEFT JOIN role_summary rs
    ON rs.source_dataset = pb.source_dataset
   AND rs.practitioner_fhir_id = pb.fhir_id
GROUP BY pb.source_dataset ORDER BY pb.source_dataset;


-- ============================================================
-- SECTION 8: PRACTITIONERROLE COUNT PER PRACTITIONER
-- Distribution of how many roles each practitioner has
-- ============================================================

WITH role_counts AS (
    SELECT
        pr.source_dataset,
        reverse(split_part(reverse(pr.raw_json->'practitioner'->>'reference'), '/', 1)) AS practitioner_fhir_id,
        COUNT(*) AS role_count
    FROM practitioner_roles pr
    WHERE pr.raw_json->'practitioner' IS NOT NULL
    GROUP BY pr.source_dataset, practitioner_fhir_id
)
SELECT
    source_dataset,
    MIN(role_count)                     AS min_roles,
    MAX(role_count)                     AS max_roles,
    ROUND(AVG(role_count), 2)           AS avg_roles,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY role_count) AS median_roles,
    COUNT(*) FILTER (WHERE role_count = 1) AS practitioners_with_1_role,
    COUNT(*) FILTER (WHERE role_count = 2) AS practitioners_with_2_roles,
    COUNT(*) FILTER (WHERE role_count >= 3) AS practitioners_with_3plus_roles
FROM role_counts
GROUP BY source_dataset ORDER BY source_dataset;


-- ============================================================
-- SECTION 9: NPI-BASED PRACTITIONER OVERLAP ACROSS DATASETS
-- Both datasets cover some real-world providers. How much overlap
-- is there? This is more meaningful than fhir_id overlap.
-- ============================================================

WITH practitioner_npis AS (
    SELECT
        source_dataset,
        elem->>'value' AS npi
    FROM practitioners,
         jsonb_array_elements(raw_json->'identifier') AS elem
    WHERE (elem->>'system' = 'http://hl7.org/fhir/sid/us-npi'
        OR elem->>'system' = 'http://terminology.hl7.org/NamingSystem/npi')
)
SELECT
    COUNT(DISTINCT npi) FILTER (WHERE source_dataset = 'dataset-a')  AS dataset_a_unique_npis,
    COUNT(DISTINCT npi) FILTER (WHERE source_dataset = 'dataset-b')  AS dataset_b_unique_npis,
    COUNT(DISTINCT npi) FILTER (
        WHERE source_dataset = 'dataset-a'
          AND npi IN (SELECT npi FROM practitioner_npis WHERE source_dataset = 'dataset-b')
    )                                                                 AS npis_in_both,
    COUNT(DISTINCT npi) FILTER (
        WHERE source_dataset = 'dataset-a'
          AND npi NOT IN (SELECT npi FROM practitioner_npis WHERE source_dataset = 'dataset-b')
    )                                                                 AS only_in_a,
    COUNT(DISTINCT npi) FILTER (
        WHERE source_dataset = 'dataset-b'
          AND npi NOT IN (SELECT npi FROM practitioner_npis WHERE source_dataset = 'dataset-a')
    )                                                                 AS only_in_b
FROM practitioner_npis;


-- ============================================================
-- SECTION 10: AVAILABLETIME (SCHEDULING DATA)
-- NDH PractitionerRole includes availability windows.
-- Presence of this data is a strong indicator of directory richness.
-- ============================================================

-- % of roles with any availability data
SELECT
    source_dataset,
    COUNT(*)                                                                                      AS total,
    COUNT(*) FILTER (WHERE raw_json->'availableTime' IS NOT NULL)                               AS has_available_time,
    COUNT(*) FILTER (WHERE raw_json->'notAvailable'  IS NOT NULL)                               AS has_not_available,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'availableTime' IS NOT NULL) / COUNT(*), 1) AS pct_available_time,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'notAvailable'  IS NOT NULL) / COUNT(*), 1) AS pct_not_available
FROM practitioner_roles
GROUP BY source_dataset ORDER BY source_dataset;

-- Days-of-week distribution in availableTime (what days are covered?)
SELECT
    source_dataset,
    day_elem::text AS day_of_week,
    COUNT(*)       AS occurrences
FROM practitioner_roles,
     jsonb_array_elements(raw_json->'availableTime') AS avail,
     jsonb_array_elements(avail->'daysOfWeek')       AS day_elem
WHERE raw_json->'availableTime' IS NOT NULL
GROUP BY source_dataset, day_of_week
ORDER BY source_dataset, day_of_week;


-- ============================================================
-- SECTION 11: IDENTIFIER SYSTEMS ON PRACTITIONERROLE
-- PractitionerRole may carry its own identifiers (role/location specific)
-- beyond the practitioner NPI (which lives on Practitioner)
-- ============================================================

SELECT
    source_dataset,
    elem->>'system' AS identifier_system,
    COUNT(*)        AS occurrences
FROM practitioner_roles,
     jsonb_array_elements(raw_json->'identifier') AS elem
WHERE raw_json->'identifier' IS NOT NULL
GROUP BY source_dataset, identifier_system
ORDER BY source_dataset, occurrences DESC;

-- % of roles with any identifier
SELECT
    source_dataset,
    COUNT(*)                                                                                      AS total,
    COUNT(*) FILTER (WHERE raw_json->'identifier' IS NOT NULL)                                  AS has_identifier,
    ROUND(100.0 * COUNT(*) FILTER (WHERE raw_json->'identifier' IS NOT NULL) / COUNT(*), 1)    AS pct_has_identifier
FROM practitioner_roles
GROUP BY source_dataset ORDER BY source_dataset;


-- ============================================================
-- SECTION 12: SPOT CHECK — SAMPLE RECORDS
-- Inspect a few raw records from each dataset to validate
-- structure assumptions and spot unexpected fields.
-- ============================================================

-- Sample practitioner from each dataset (with roles)
SELECT
    p.source_dataset,
    p.fhir_id,
    p.raw_json->>'active'                                          AS active,
    p.raw_json->'name'->0->>'text'                                AS name,
    p.raw_json->>'gender'                                          AS gender,
    (SELECT elem->>'value'
     FROM jsonb_array_elements(p.raw_json->'identifier') elem
     WHERE elem->>'system' IN (
         'http://hl7.org/fhir/sid/us-npi',
         'http://terminology.hl7.org/NamingSystem/npi')
     LIMIT 1)                                                      AS npi,
    jsonb_array_length(p.raw_json->'qualification')               AS qual_count,
    jsonb_array_length(p.raw_json->'extension')                   AS ext_count,
    (SELECT COUNT(*) FROM practitioner_roles pr
     WHERE pr.source_dataset = p.source_dataset
       AND reverse(split_part(reverse(pr.raw_json->'practitioner'->>'reference'), '/', 1)) = p.fhir_id
    )                                                              AS role_count
FROM practitioners p
WHERE p.raw_json->'qualification' IS NOT NULL
ORDER BY p.source_dataset, p.fhir_id
LIMIT 10;

-- Sample PractitionerRole records
SELECT
    source_dataset,
    fhir_id,
    raw_json->>'active'                                  AS active,
    raw_json->'practitioner'->>'reference'              AS practitioner_ref,
    raw_json->'organization'->>'reference'              AS organization_ref,
    raw_json->'specialty'->0->'coding'->0->>'code'     AS primary_specialty_code,
    raw_json->'specialty'->0->'coding'->0->>'display'  AS primary_specialty_display,
    jsonb_array_length(raw_json->'location')            AS location_count,
    raw_json->'availableTime' IS NOT NULL               AS has_available_time,
    jsonb_array_length(raw_json->'extension')           AS ext_count
FROM practitioner_roles
ORDER BY source_dataset, fhir_id
LIMIT 10;
