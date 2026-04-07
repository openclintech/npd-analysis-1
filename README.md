# National Provider Directory — Dataset Evaluation Findings

**Purpose:** Evaluate dataset-a vs dataset-b for CMS publication recommendation  
**Deadline:** Monday, April 6, 2026  
**IG:** [NDH Implementation Guide](https://build.fhir.org/ig/HL7/fhir-us-ndh/)  
**Pipeline:** GCS → Go ingestion → Cloud SQL (PostgreSQL) → Analysis  
**NOTE:** To access the NDJSON files, you must join the CMS Health Tech Ecosystem. However, they will soon be available as public use files (PUF) once they are out of beta.

## CMS Feedback Focus Areas
CMS has specifically requested feedback on:
1. **Data processing strategy** — methods and normalization approaches used in each dataset
2. **Organization hierarchies** — how parent/child org relationships are determined
3. **Practitioner → organization relationships** — how PractitionerRole links are established
4. **Source data combination** — how CMS, HHS, and industry partner data is merged
5. **Dataset preference** — which dataset would you choose and why

These focus areas are used to structure the findings below.

---

---

## Pipeline Overview

| Component | Detail |
|---|---|
| Source | Google Cloud Storage bucket `national-provider-directory` |
| Datasets | `dataset-a` (zstd compression), `dataset-b` (7z compression) |
| Resources | Organization, OrganizationAffiliation, Practitioner, PractitionerRole, Location, Endpoint |
| Destination | Cloud SQL (PostgreSQL, us-central1) |
| Ingestion | Go, batch inserts, concurrent per-dataset within each resource group |

---

## 1. Compression & Pipeline Performance

### Compression Format
- **Dataset-a:** zstd (`.ndjson.zst`)
- **Dataset-b:** 7z (`.ndjson.7z`)

**Finding:** zstd is significantly better for pipeline use.
- zstd supports streaming directly from GCS — no intermediate storage needed
- 7z requires downloading the entire file to a temp file before decompression (requires `io.ReaderAt`)
- For a production pipeline running repeatedly, zstd reduces disk I/O, memory pressure, and latency

### File Sizes & Ingestion Performance

| Resource | A Size | B Size | Size Ratio | A Rec/s | B Rec/s | A Time | B Time |
|---|---|---|---|---|---|---|---|
| Organization | 747 MB | 76.7 MB | 9.7x | 1,683 | 3,535 | 83m | 9m |
| OrgAffiliation | 17.3 MB | 2.2 MB | 7.9x | 5,266 | 4,317 | 1m 20s | 5s |
| Practitioner | 6.8 GB | 91.4 MB | 76x | 296 | 2,281 | **419m** | 10m |
| PractitionerRole | 297 MB | 189 MB | 1.6x | 5,560 | 4,484 | 26m | 15m |
| Location | 709 MB | 50.6 MB | 14x | 5,271 | 4,488 | 41m | 3m |
| Endpoint | 180 MB | 1.6 MB | 113x | 3,145 | 7,287 | 27m | 10s |
| **Total** | **~9 GB** | **~411 MB** | **~22x** | — | — | **~10 hrs** | **~38 min** |

**Finding:** Dataset-a took ~10 hours to ingest vs ~38 minutes for dataset-b — a 16x difference in total pipeline time. The practitioner file alone took 7 hours (419 minutes) due to large record sizes (~10KB/record). Dataset-b ingests in under an hour, making it far more operationally practical for a recurring pipeline.

**Practitioner throughput difference:** Dataset-a processes at 296 rec/s vs dataset-b's 2,281 rec/s — nearly 8x slower — directly attributable to record size (dataset-a practitioner records average ~10KB vs much smaller in dataset-b).

---

## 2. Record Counts

> Note: Full ingestion still in progress. Counts will be updated when complete.

| Resource | Dataset A | Dataset B | Ratio (A/B) |
|---|---|---|---|
| Organization | 8,398,280 | 1,947,042 | 4.3x |
| OrganizationAffiliation | 424,977 | 20,531 | 20.7x |
| Practitioner | 7,441,212 | 1,329,000 | 5.6x |
| PractitionerRole | 8,805,588 | 4,018,225 | 2.2x |
| Location | 12,887,406 | 879,987 | 14.6x |
| Endpoint | 5,070,461 | 63,498 | 79.9x |
| **TOTAL** | **43,027,924** | **8,258,283** | **5.2x** |

**Finding:** Dataset-a has 5.2x more records overall. The most dramatic differences are in Endpoint (68x) and OrganizationAffiliation (20x), which are critical for directory usability. Location is 14.6x larger. All files ingested with 0 errors.

---

## 3. Ingestion Issues & Observations

### Scanner Buffer Overflow (Dataset-a)
- **Issue:** Dataset-a organization file contains records exceeding 10MB per line, causing ingestion to crash
- **Root cause:** Individual JSON records with abnormally large array fields (see Section 4)
- **Resolution:** Increased scanner buffer from 10MB to 100MB
- **Dataset-b:** No such issue — all records within normal size bounds

### Connection Drops on Long-Running Files
Two distinct connection issues encountered during ingestion of large dataset-a files:

**Issue 1: DB connection reset (practitioner_A, ~750k records)**
- The Cloud SQL proxy reset TCP connections held open for extended periods
- Root cause: single connection held for 45+ minutes during slow practitioner ingestion (~280 rec/s)
- Resolution: changed to acquire a fresh DB connection from the pool per batch flush rather than holding one for the entire file

**Issue 2: Proxy idle timeout + GCS stream EOF (organization_A, ~5.27M records)**
- Proxy connection to Cloud SQL timed out after ~15 minutes of low activity
- Proxy log: `connection aborted - error reading from instance: read: operation timed out`
- Followed by `network is unreachable` on reconnect attempt (brief network blip)
- GCS stream also returned `unexpected EOF` when the underlying connection dropped
- Resolution: 
  - Added `RetryAlways` policy to GCS object reader for automatic stream retry
  - Added `MaxConnLifetime = 5min` and `HealthCheckPeriod = 30s` to pool config so connections are recycled before proxy idle timeout

**Dataset-b:** Not affected — all files are small enough to complete before any timeout threshold.

### Previous Run Errors
- An earlier ingestion run (before schema fix) had millions of errors due to a missing composite unique constraint. The original `organizations` table only had `UNIQUE(fhir_id)` instead of `UNIQUE(fhir_id, source_dataset)`, causing concurrent upserts from both datasets to conflict.
- After schema correction and re-run, errors dropped to 0.

### Clean Ingestion Results (confirmed 0 errors across all 12 files)
| File | Records | Process Time |
|---|---|---|
| organization_A | 8,398,280 | 83m 8s |
| organization_B | 1,947,042 | 9m 10s |
| organization_affiliation_A | 424,977 | 1m 20s |
| organization_affiliation_B | 20,540 | 4.8s |
| practitioner_A | 7,441,212 | 419m 16s |
| practitioner_B | 1,329,000 | 9m 42s |
| practitioner_role_A | 8,805,588 | 26m 23s |
| practitioner_role_B | 4,018,225 | 14m 56s |
| location_A | 12,887,406 | 40m 45s |
| location_B | 880,000 | 3m 16s |
| endpoint_A | 5,070,461 | 26m 52s |
| endpoint_B | 74,499 | 10.2s |

---

## 4. Data Quality Findings

### Critical: Corrupted Records in Dataset-a (Organizations)

Three organization records in dataset-a have severely bloated array fields, consistent with a data generation bug where data from multiple locations was incorrectly merged into a single record:

| Organization | Addresses | Contacts | Telecoms | Identifiers | Record Size |
|---|---|---|---|---|---|
| WALGREENS 1008 | 36,486 | 30,863 | 18,218 | 11,768 | 31 MB (raw JSON) |
| WALMART STORES EAST LP | 11,408 | 8,359 | 5,743 | 4,981 | ~8.4 MB |
| LENSCRAFTERS INC 118 | 8,839 | 4,921 | 3,357 | 3,232 | ~5.2 MB |

**Key observations:**
- All three are retail chain locations (pharmacy/optical) — suggesting a systematic bug affecting a specific category of organization
- None of these records exist in dataset-b — dataset-b either excluded them or was generated from a cleaner pipeline
- A legitimate organization record should have O(1–10) addresses, not O(10,000–36,000)
- These records would be harmful to publish — consuming applications would receive tens of thousands of addresses for a single location

**Recommendation:** These records should be investigated and excluded from any public release of dataset-a. CMS should determine whether additional records of this type exist beyond the three identified here.

---

## 5. Preliminary Comparison Summary

| Dimension | Dataset A | Dataset B |
|---|---|---|
| Compression | zstd ✅ better for pipelines | 7z ❌ requires temp file |
| Record volume | Higher (2.5x+ organizations) | Lower |
| Record size | Larger, more verbose JSON | Smaller, more consistent |
| Data quality | Known corrupted records ❌ | Clean so far ✅ |
| Ingestion errors | Buffer overflow on oversized records | None |
| Pipeline complexity | Simpler (streaming) | More complex (temp file) |

---

## 6. CMS-Specific Observations (Preliminary)

### Data Processing Strategy (CMS Focus Area #1)

The two datasets represent **fundamentally different processing models** for organization data:

**Dataset-a: Aggregation Model**
- Aggregates attributes across multiple source systems into organization records
- Replicates attributes across multiple org records sharing the same NPI
- Does not resolve identity — the same real-world provider can appear as multiple distinct Organization resources
- Produces a fragmented representation where ~54% of records are "shell" records with minimal data, and ~40.6% are fully directory-ready
- Rich in attributes where populated, but noisy and not deduplicated

**Dataset-b: Normalization Model**
- One row per NPI — strict identity resolution
- Minimal transformation applied; conforms to US Core Organization profile (not NDH)
- 100% NPI coverage, 100% name coverage, 0% shell records
- Clean identity layer with no fragmentation
- Sparse on directory attributes (telecom, address, type, endpoint all at 0%)

**Key Insight:** Dataset-a's higher row count (~8.4M vs ~1.95M) is driven by structural duplication and aggregation, **not** increased real-world coverage. Both datasets cover approximately the same universe of ~2M distinct NPIs. The difference is that dataset-a represents one NPI as multiple Organization records, while dataset-b represents each NPI exactly once.

### Organization Hierarchies (CMS Focus Area #2)
Dataset-a shows evidence of a **data aggregation bug in organization hierarchy processing**. At least three retail chain locations (Walgreens, Walmart, LensCrafters) have records where array fields contain tens of thousands of entries — consistent with every location in a chain being incorrectly rolled up into a single record:

| Organization | Addresses | Contacts | Telecoms | Identifiers | Raw Record Size |
|---|---|---|---|---|---|
| WALGREENS 1008 | 36,486 | 30,863 | 18,218 | 11,768 | 31 MB |
| WALMART STORES EAST LP | 11,408 | 8,359 | 5,743 | 4,981 | ~8.4 MB |
| LENSCRAFTERS INC 118 | 8,839 | 4,921 | 3,357 | 3,232 | ~5.2 MB |

- These records **do not exist in dataset-b** — dataset-b either excluded or never generated them
- A legitimate single-location organization should have O(1–10) addresses
- This suggests dataset-a's organization hierarchy logic is incorrectly merging chain locations

**NPI Fragmentation (broader issue):** Beyond the corrupted records, dataset-a has a systemic identity problem: a single NPI maps to multiple distinct Organization resources. For example, NPI `1417236688` maps to multiple organizations (MED ZONE PHARMACY, MEDRX LLC, KERRY EASTLAND ENTERPRISES, etc.) that share the same address, telecom, and contacts — the same physical entity represented as multiple FHIR resources. This is entity aggregation without identity resolution.

Expected semantic: 1 NPI → 1 Organization  
Observed in dataset-a: 1 NPI → multiple Organizations

This means downstream consumers cannot reliably look up "the organization with NPI X" — they will get multiple conflicting results. Organization hierarchy (partOf) relationships are correspondingly unclear, since the parent entity itself may be fragmented.

Dataset-b avoids this entirely: one row per NPI, unambiguous identity.

### Practitioner → Organization Relationships (CMS Focus Area #3)
OrganizationAffiliation record counts differ dramatically:
- Dataset-a: **424,977** affiliation records
- Dataset-b: **20,540** affiliation records (~20x fewer)

This is one of the most significant differences observed. It suggests the two datasets use fundamentally different approaches to modeling practitioner-organization relationships. Dataset-a's affiliations are **ambiguous** — because organization identity is fragmented, it's unclear which Organization record a practitioner is actually affiliated with when multiple records share the same NPI. Dataset-b's affiliations are **simpler and more reliable** due to its one-NPI-per-Organization identity model. Full referential integrity analysis (pending) will show how many of these affiliations actually resolve to valid practitioners and organizations.

### Source Data Combination (CMS Focus Area #4)
The massive record count differences across all resource types suggest dataset-a is drawing from a broader set of sources or applying less aggressive deduplication/filtering. The NPI fragmentation analysis confirms this: dataset-a's ~8.4M organization records cover only ~2M distinct NPIs — the same real-world coverage as dataset-b's ~1.95M records, but expressed as ~4x more rows due to aggregation without deduplication. Combining sources without identity resolution introduces structural duplication that inflates counts without adding coverage.

The corrupted organization records (Walgreens, Walmart, LensCrafters) further illustrate the risk: combining sources incorrectly merges data from multiple locations into single records, producing records with tens of thousands of array entries.

### Dataset Preference (CMS Focus Area #5)
> **Preliminary:** Full analysis still pending. To be finalized after ingestion completes and analysis.sql runs.

Based on findings so far, neither dataset alone satisfies NDH must-support requirements, but the datasets have complementary strengths:

| Dimension | Dataset A | Dataset B |
|---|---|---|
| Processing model | Aggregation (fragmented) | Normalization (clean) |
| NPI fragmentation | 1 NPI → multiple orgs ❌ | 1 NPI → 1 org ✅ |
| Shell records | ~54% minimal data ❌ | 0% ✅ |
| Directory attributes | ~40.6% populated | 0% populated ❌ |
| FHIR profile | NDH-aligned | US Core (not NDH) ❌ |
| Corrupted records | Yes (at minimum 3) ❌ | None identified ✅ |

**Dataset-a** is richer in attributes but fragmented, noisy, and contains data quality defects. It requires significant post-processing (NPI-based deduplication, attribute merging, corrupted record removal) before it would be usable as a public directory.

**Dataset-b** is clean and identity-resolved but lacks the directory attributes (address, telecom, type, endpoint) that make an organization record usable for actual provider lookup. It would require external enrichment.

**For public release**, dataset-b is the safer baseline — it is clean, consistent, and will not break consuming applications. Dataset-a's coverage advantage is real but overstated by structural duplication; its data quality issues make it unsuitable for publication without remediation.

---

## 7. Analysis Results

### Organization Field Completeness (NDH Must-Support)
| Field | Dataset A | Dataset B |
|---|---|---|
| active | 100% | **0%** |
| type | 23.8% | **0%** |
| name | 46% | **100%** |
| telecom | 40.6% | **0%** |
| address | 41% | **0%** |
| endpoint ref | 1.1% | **0%** |

**Finding:** The two datasets are near polar opposites in field coverage for organizations. Dataset-b has 100% name coverage but is missing every other must-support field. Dataset-a has 100% active coverage but only 46% name and sparse coverage elsewhere. Neither dataset alone fully satisfies NDH must-support requirements for organizations. This is a significant finding — it suggests the two datasets are drawing from fundamentally different source systems or applying very different normalization strategies.

### Dataset-b Organization Record Inspection
Manual inspection of dataset-b organization records revealed several important structural observations:

**1. Profile conformance: US Core, not NDH**
Dataset-b organizations conform to `http://hl7.org/fhir/us/core/StructureDefinition/us-core-organization` (US Core), not the NDH IG profile. This directly explains the missing must-support fields — US Core Organization has a much smaller set of required fields than NDH Organization. The two datasets are conforming to **different FHIR profiles**, which is a fundamental data processing strategy difference.

**2. Non-standard `partOf` references**
Dataset-b uses full absolute URLs for `partOf` references:
```
"reference": "http://dev.cnpd.internal.cms.gov/fhir/Organization/14144c5e-..."
```
Standard FHIR uses relative references (`Organization/id`). This means referential integrity queries using standard FHIR reference parsing will fail for dataset-b. The `dev.cnpd.internal.cms.gov` hostname also indicates these references point to a CMS internal development system.

**3. CMS internal origin**
The `dev.cnpd.internal.cms.gov` domain in reference URLs suggests dataset-b was generated from a CMS internal system, consistent with CMS's description of two independently produced datasets.

**4. Qualification on Organization (non-standard)**
Some dataset-b organizations have a qualification extension, which is normally a Practitioner field in FHIR. This suggests dataset-b may be modeling some provider-level data at the organization level.

**5. Minimal fields by design**
Given the US Core profile conformance, dataset-b's sparse field coverage is intentional — not a data quality gap. It's a different modeling approach: fewer fields, all correctly populated vs. more fields, inconsistently populated.

### NPI Coverage (Organizations)
| Dataset | Total | Has NPI | % Coverage |
|---|---|---|---|
| dataset-a | 8,398,280 | 3,411,748 | 40.6% |
| dataset-b | 1,947,042 | 1,947,042 | 100% |

**Finding:** Dataset-b has 100% NPI coverage for organizations. Dataset-a has only 40.6% — meaning ~4.9M organizations have no NPI identifier. This is a critical data quality gap. NPI is the primary identifier for provider lookup in the US healthcare system. Organizations without NPI are effectively unsearchable by the most common lookup method, severely limiting the usability of dataset-a for directory purposes.

Investigation of the non-NPI organizations in dataset-a revealed multiple data quality issues:
- They have only TAX identifiers using `http://hl7.org/fhir/sid/us-ssn` as the system — SSN is for individuals, not organizations
- The TAX identifier values are UUIDs (e.g. `44a7fe64-f5a0-412f-a3af-e6c8b13e5a06`), not real EINs or NPIs
- Some records have duplicate identical identifiers listed twice
- These appear to be synthetic/placeholder identifiers rather than real provider identifiers

This suggests the ~4.9M non-NPI organizations in dataset-a may not represent real credentialed providers, which would partially explain the large record count gap between the two datasets.

### NPI Fragmentation (Dataset-a Organizations)

Dataset-a has ~8.4M organization rows but only ~2.0M distinct NPIs — meaning the same NPI appears across multiple distinct Organization resources. This is structural duplication, not additional coverage.

**Deep-dive example — NPI 1417236688:**
Multiple Organization records share this NPI:
- MED ZONE PHARMACY
- MEDRX LLC
- KERRY EASTLAND ENTERPRISES
- (additional records)

All share the same address, telecom, and contact data. These represent the same physical provider expressed as separate FHIR resources — entity aggregation without identity resolution.

**Implications:**
- A consumer querying "Organization where NPI = X" gets multiple conflicting results
- PractitionerRole and OrganizationAffiliation references into dataset-a are ambiguous — it's unclear which Organization record is authoritative
- Organization hierarchy (partOf) relationships are unreliable when the parent entity itself is fragmented

Dataset-b has no fragmentation: ~1.95M rows, ~1.95M distinct NPIs. One row per NPI, unambiguous identity.

**Both datasets cover approximately the same real-world provider universe (~2M NPIs).** Dataset-a's 4x higher row count is structural duplication, not broader coverage.

### Shell Records (Dataset-a Organizations)

Approximately **54% of dataset-a organization records are "shell" records** — they contain minimal data (NPI + name only, or NPI alone) with no telecom, address, type, or other directory attributes. These records exist in the dataset but are not usable for directory lookup.

Combined with the NPI fragmentation finding, the ~40.6% "fully directory-ready" figure for dataset-a organizations represents a subset of records that have both NPI and populated directory attributes. The majority of dataset-a organization records are either shells or duplicates of other records sharing the same NPI.

### Practitioner Field Completeness (NDH Must-Support)
| Field | Dataset A | Dataset B |
|---|---|---|
| active | **100%** | **0%** |
| name | 100% | 100% |
| gender | **100%** | **0%** |
| identifier (any) | 100% | 100% |
| telecom | 100% | 99.8% |
| address | 100% | 53.6% |
| qualification | 95.8% | **100%** |
| communication | 2.5% | 0% |

**Findings:**
- Dataset-a is highly complete on practitioner demographics: 100% across active, name, gender, telecom, and address — unusually comprehensive for a bulk dataset
- Dataset-b is missing `active` and `gender` entirely (same pattern as organizations — US Core profile does not require them)
- Dataset-b places **telecom (99.8%) and address (53.6%) directly on the Practitioner resource** — this is a notable modeling choice. NDH convention is to express contact info via PractitionerRole (telecom on the role, address via referenced Location), not on the Practitioner itself. Dataset-b is modeling provider contact information at the person level rather than the role/location level.
- Dataset-b has marginally better qualification coverage (100% vs 95.8%)
- Neither dataset populates communication well (2.5% and 0%)

### Practitioner NDH Extensions
| Extension | Dataset A | Dataset B |
|---|---|---|
| any extension | 7,441,212 (100%) | 0 (0%) |
| endpoint-reference | 2,417,045 (32.5%) | 0 |
| verification-status | 0 | 0 |
| communication-proficiency | 0 | 0 |
| rating | 0 | 0 |

**Findings:**
- Dataset-a populates extensions on every Practitioner record. The only extension used is `endpoint-reference`, which links a practitioner directly to an Endpoint resource — present on ~32.5% of practitioners. This is an NDH-specific extension for direct technical connectivity.
- Dataset-b has **zero extensions on any Practitioner record**. This is consistent with US Core profile conformance — US Core Practitioner does not define these NDH extensions. It is not a data gap but a consequence of profiling to a different IG.
- Neither dataset populates verification-status, communication-proficiency, or rating — three NDH extensions that would significantly increase directory value (data quality tracking, language access, and provider reputation).

### NPI Coverage (Practitioners)
| Dataset | Total | Has NPI | % Coverage |
|---|---|---|---|
| dataset-a | 7,441,212 | 7,441,212 | 100% |
| dataset-b | 1,329,000 | 1,329,000 | 100% |

**Finding:** Both datasets have 100% NPI coverage for practitioners — a strong positive. However they use **different system URLs** for the NPI identifier:
- Dataset-a: `http://hl7.org/fhir/sid/us-npi` (HL7 FHIR standard URL)
- Dataset-b: `http://terminology.hl7.org/NamingSystem/npi` (HL7 terminology server URL)

Both URLs refer to NPI but the inconsistency is a normalization difference between the two processing pipelines. Any consuming application querying by NPI system URL would need to handle both — or one dataset would return zero results. This is directly relevant to CMS's question about data processing strategy and normalization approaches.

---

## 8. Analysis Still Pending

The following analysis was scoped but not completed before submission. It may be pursued in a follow-up if CMS requests deeper investigation.

- [ ] PractitionerRole field completeness (specialty, location, telecom, endpoint, availableTime, NDH extensions)
- [ ] Referential integrity: PractitionerRole → Practitioner and PractitionerRole → Organization resolution rates
- [ ] NUCC specialty coding coverage on PractitionerRole
- [ ] NDH extension population on PractitionerRole (newpatients, network participation)
- [ ] Joined directory completeness: % of practitioners with NPI + name + role + specialty + contact
- [ ] NPI-based practitioner overlap across datasets (how many real-world providers appear in both)
- [ ] Roles per practitioner distribution
- [ ] Data freshness (lastUpdated timestamps across all resource types)
- [ ] Additional corrupted record sweep in dataset-a beyond the 3 identified organizations
- [ ] OrganizationAffiliation role code analysis
- [ ] Location and Endpoint field completeness

---

## 9. Tentative Conclusions (CMS Feedback)

> These conclusions are based on completed ingestion of all 12 files (43M records across dataset-a, 8.3M across dataset-b) and analysis of Organization and Practitioner resources. PractitionerRole, Location, Endpoint, and OrganizationAffiliation analysis is still in progress. Conclusions may be refined if that analysis surfaces new findings.

---

### CMS Focus Area #1: Data Processing Strategy

The two datasets use **fundamentally different processing models**:

**Dataset-a** is an **aggregation model**: it draws from multiple source systems, combines attributes across sources into individual records, and does not resolve provider identity before generating FHIR resources. The result is a large, attribute-rich dataset (~43M records) that is fragmented along NPI lines. It conforms to the NDH IG profile, uses standard FHIR relative references, and compresses with zstd (streaming-friendly). NPI is expressed using the standard HL7 FHIR system URL (`http://hl7.org/fhir/sid/us-npi`).

**Dataset-b** is a **normalization model**: it enforces one record per NPI (for organizations), applies strict identity resolution, and produces a smaller, cleaner dataset (~8.3M records). It conforms to the **US Core profile** — not the NDH IG — which explains its sparse coverage of NDH must-support fields. It uses absolute CMS-internal URLs for FHIR references (`http://dev.cnpd.internal.cms.gov/fhir/...`), which will break standard relative-reference parsing by consuming applications. NPI is expressed using a different system URL (`http://terminology.hl7.org/NamingSystem/npi`).

The NPI system URL inconsistency between the two datasets is a concrete normalization gap: a consuming application querying practitioners by NPI identifier URL would need to know which system URL each dataset uses, or queries against one dataset would return zero results.

---

### CMS Focus Area #2: Organization Hierarchies

**Dataset-a** models organization hierarchies using FHIR `partOf` references in the standard relative format. However, the NPI fragmentation problem (one NPI mapping to multiple Organization records) undermines hierarchy reliability — when the parent entity itself may be duplicated, the parent/child tree is ambiguous. Additionally, at least three records (Walgreens, Walmart, LensCrafters) show evidence of an aggregation bug where every location in a chain was incorrectly merged into a single record, producing organizations with tens of thousands of addresses and contacts. These do not exist in dataset-b and suggest the chain/hierarchy logic has a defect for retail-style provider networks.

**Dataset-b** uses `partOf` references with absolute CMS-internal URLs, which will not resolve using standard FHIR reference parsing. The one-NPI-per-Organization identity model is correct in principle for hierarchy, but the non-standard reference format is a practical barrier for consumers.

Neither dataset's hierarchy representation is production-ready without remediation.

---

### CMS Focus Area #3: Practitioner → Organization Relationships

Both datasets use PractitionerRole as the primary resource linking practitioners to organizations, which is correct per FHIR and NDH.

**Dataset-a** has 8.8M PractitionerRole records and 425K OrganizationAffiliation records. Practitioner demographics are comprehensive (100% NPI, name, gender, active, telecom, address). However, the NPI fragmentation in organizations creates ambiguity: when a PractitionerRole references an organization by FHIR ID, and that organization's NPI maps to 4+ duplicate Organization records, it is unclear which record is authoritative. The `endpoint-reference` extension is populated on 32.5% of Practitioner records, providing direct technical connectivity links.

**Dataset-b** has 4M PractitionerRole records and only 20.5K OrganizationAffiliation records (~20x fewer). Practitioner records have 100% NPI, name, and qualification coverage, but are missing `active` and `gender` (US Core does not require them). Dataset-b models telecom (99.8%) and address (53.6%) directly on the Practitioner resource — NDH convention is to express contact info at the PractitionerRole level via referenced Location. This is a modeling strategy difference. No extensions are populated on any dataset-b Practitioner record.

The 20x difference in OrganizationAffiliation records is the most significant gap between the datasets for this focus area and warrants investigation by the respective dataset producers.

---

### CMS Focus Area #4: Source Data Combination

**Dataset-a** appears to draw from a broader set of sources with less aggressive deduplication. The evidence:
- ~8.4M organization records covering only ~2M distinct NPIs — the same real-world footprint as dataset-b, expressed as 4x more rows through structural duplication
- ~4.9M organizations (59.4%) have no NPI at all; their TAX identifiers use SSN system URLs and UUID values, suggesting synthetic or placeholder data from a source that does not map to real credentialed providers
- The corrupted Walgreens/Walmart/LensCrafters records indicate that source combination without sufficient validation can produce records that would break consuming applications

**Dataset-b** draws from what appears to be a narrower, higher-confidence source (likely CMS's own provider enrollment data, suggested by the `dev.cnpd.internal.cms.gov` reference URLs). All organization records have a valid NPI. No corrupted records were identified. The tradeoff is lower coverage of directory attributes — the dataset functions as a clean identity layer but not a usable provider directory on its own.

---

### CMS Focus Area #5: Dataset Preference

**Neither dataset alone is ready for public release as a production NDH directory.** They have complementary strengths and different failure modes.

| Dimension | Dataset A | Dataset B |
|---|---|---|
| Profile conformance | NDH ✅ | US Core (not NDH) ❌ |
| Compression | zstd (streaming) ✅ | 7z (requires temp file) ❌ |
| Practitioner NPI | 100% ✅ | 100% ✅ |
| Organization NPI | 40.6% ❌ | 100% ✅ |
| NPI fragmentation (orgs) | 1 NPI → multiple orgs ❌ | 1 NPI → 1 org ✅ |
| Practitioner demographics | Complete ✅ | Missing active, gender ❌ |
| Directory attributes (orgs) | Partially populated | 0% (name only) ❌ |
| Corrupted records | Yes (at minimum 3) ❌ | None identified ✅ |
| FHIR reference format | Standard relative ✅ | Absolute CMS-internal URLs ❌ |
| NPI system URL | Standard HL7 FHIR ✅ | Non-standard terminology URL ⚠️ |
| OrganizationAffiliations | 425K | 20.5K (20x fewer) ⚠️ |

**If forced to choose one for publication today, dataset-b is the safer baseline.** It is clean, identity-resolved, and will not send malformed records to consuming applications. Its gaps are absences (missing fields) rather than errors (corrupted data), which is a more recoverable failure mode.

**Dataset-a has broader real-world coverage potential** but requires significant remediation before it is safe to publish: NPI-based deduplication of organization records, removal or repair of the corrupted chain records, and investigation of the ~4.9M non-NPI organizations. Until that work is done, dataset-a's apparent scale advantage is partially illusory and partially hazardous.

**The strongest long-term recommendation** is to reconcile the two datasets rather than choose one. Dataset-b's clean NPI-resolved identity layer combined with dataset-a's directory attributes (telecom, address, qualification, specialty) would produce a dataset that satisfies NDH must-support requirements more fully than either does independently. This also requires resolving the FHIR profile disagreement — both datasets should conform to NDH, not US Core.

---

*Last updated: 2026-04-04. Submitted to CMS as working findings. Further analysis may follow.*
