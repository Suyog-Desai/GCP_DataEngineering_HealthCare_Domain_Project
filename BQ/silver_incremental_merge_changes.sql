-------------------------------------------------------------------------------------------------------
-- PATIENTS
-------------------------------------------------------------------------------------------------------

-- 1. Create patients Table in BigQuery
CREATE TABLE IF NOT EXISTS `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.patients`(
  Patient_Key STRING,
  SRC_PatientID STRING,
  FirstName STRING,
  LastName STRING,
  MiddleName STRING,
  SSN STRING,
  PhoneNumber STRING,
  Gender STRING,
  DOB INT64,
  Address STRING,
  SRC_ModifiedDate INT64,
  datasource STRING,
  is_quarantined BOOL,
  inserted_date TIMESTAMP,
  modified_date TIMESTAMP,
  is_current BOOL);

-- 2. Create a quality_checks temp table
CREATE OR REPLACE TABLE `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.quality_checks`
AS
SELECT DISTINCT
  CONCAT(SRC_PatientID, '-', datasource) AS Patient_Key,
  SRC_PatientID,
  FirstName,
  LastName,
  MiddleName,
  SSN,
  PhoneNumber,
  Gender,
  DOB,
  Address,
  ModifiedDate AS SRC_ModifiedDate,
  datasource,
  CASE
    WHEN
      SRC_PatientID IS NULL
      OR DOB IS NULL
      OR FirstName IS NULL
      OR LOWER(FirstName) = 'null'
      THEN TRUE
    ELSE FALSE
    END AS is_quarantined
FROM
  (
    SELECT DISTINCT
      PatientID AS SRC_PatientID,
      FirstName,
      LastName,
      MiddleName,
      SSN,
      PhoneNumber,
      Gender,
      DOB,
      Address,
      ModifiedDate,
      'hospa' AS datasource
    FROM `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.patients_ha`
    UNION ALL
    SELECT DISTINCT
      ID AS SRC_PatientID,
      F_Name AS FirstName,
      L_Name AS LastName,
      M_Name AS MiddleName,
      SSN,
      PhoneNumber,
      Gender,
      DOB,
      Address,
      ModifiedDate,
      'hospb' AS datasource
    FROM `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.patients_hb`
  );

-- 3. Apply SCD Type 2 Logic — Step 1: expire changed current records
MERGE INTO `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.patients` AS target
USING `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.quality_checks` AS source
ON
  target.Patient_Key = source.Patient_Key
  AND target.is_current
    = TRUE
      WHEN
        MATCHED
        AND (
          target.SRC_PatientID IS DISTINCT FROM source.SRC_PatientID
          OR target.FirstName IS DISTINCT FROM source.FirstName
          OR target.LastName IS DISTINCT FROM source.LastName
          OR target.MiddleName IS DISTINCT FROM source.MiddleName
          OR target.SSN IS DISTINCT FROM source.SSN
          OR target.PhoneNumber IS DISTINCT FROM source.PhoneNumber
          OR target.Gender IS DISTINCT FROM source.Gender
          OR target.DOB IS DISTINCT FROM source.DOB
          OR target.Address IS DISTINCT FROM source.Address
          OR target.SRC_ModifiedDate IS DISTINCT FROM source.SRC_ModifiedDate
          OR target.datasource IS DISTINCT FROM source.datasource
          OR target.is_quarantined IS DISTINCT FROM source.is_quarantined)
        THEN UPDATE
SET
  target.is_current = FALSE,
  target.modified_date = CURRENT_TIMESTAMP();

-- Step 2: insert new patients AND new versions of changed patients
INSERT INTO `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.patients`
  (
    Patient_Key,
    SRC_PatientID,
    FirstName,
    LastName,
    MiddleName,
    SSN,
    PhoneNumber,
    Gender,
    DOB,
    Address,
    SRC_ModifiedDate,
    datasource,
    is_quarantined,
    inserted_date,
    modified_date,
    is_current)
SELECT
  source.Patient_Key,
  source.SRC_PatientID,
  source.FirstName,
  source.LastName,
  source.MiddleName,
  source.SSN,
  source.PhoneNumber,
  source.Gender,
  source.DOB,
  source.Address,
  source.SRC_ModifiedDate,
  source.datasource,
  source.is_quarantined,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP(),
  TRUE
FROM `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.quality_checks` AS source
LEFT JOIN `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.patients` AS target
  ON source.Patient_Key = target.Patient_Key AND target.is_current = TRUE
WHERE target.Patient_Key IS NULL;

-- 4. DROP quality_checks table (after both MERGE/INSERT steps use it)
DROP TABLE IF EXISTS `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.quality_checks`;

-------------------------------------------------------------------------------------------------------
-- TRANSACTIONS
-------------------------------------------------------------------------------------------------------

-- 1. Create transactions Table in BigQuery
CREATE TABLE IF NOT EXISTS `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.transactions`(
  Transaction_Key STRING,
  SRC_TransactionID STRING,
  EncounterID STRING,
  PatientID STRING,
  ProviderID STRING,
  DeptID STRING,
  VisitDate INT64,
  ServiceDate INT64,
  PaidDate INT64,
  VisitType STRING,
  Amount FLOAT64,
  AmountType STRING,
  PaidAmount FLOAT64,
  ClaimID STRING,
  PayorID STRING,
  ProcedureCode INT64,
  ICDCode STRING,
  LineOfBusiness STRING,
  MedicaidID STRING,
  MedicareID STRING,
  SRC_InsertDate INT64,
  SRC_ModifiedDate INT64,
  datasource STRING,
  is_quarantined BOOL,
  inserted_date TIMESTAMP,
  modified_date TIMESTAMP,
  is_current BOOL);

-- 2. Create a quality_checks temp table
CREATE OR REPLACE TABLE `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.quality_checks`
AS
SELECT DISTINCT
  CONCAT(TransactionID, '-', datasource) AS Transaction_Key,
  TransactionID AS SRC_TransactionID,
  EncounterID,
  PatientID,
  ProviderID,
  DeptID,
  VisitDate,
  ServiceDate,
  PaidDate,
  VisitType,
  Amount,
  AmountType,
  PaidAmount,
  ClaimID,
  PayorID,
  ProcedureCode,
  ICDCode,
  LineOfBusiness,
  MedicaidID,
  MedicareID,
  InsertDate AS SRC_InsertDate,
  ModifiedDate AS SRC_ModifiedDate,
  datasource,
  CASE
    WHEN
      EncounterID IS NULL
      OR PatientID IS NULL
      OR TransactionID IS NULL
      OR VisitDate IS NULL
      THEN TRUE
    ELSE FALSE
    END AS is_quarantined
FROM
  (
    SELECT DISTINCT *, 'hospa' AS datasource
    FROM `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.transcations_ha`
    UNION ALL
    SELECT DISTINCT *, 'hospb' AS datasource
    FROM `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.transcations_hb`
  );

-- 3. Apply SCD Type 2 Logic — Step 1: expire changed current records
MERGE
  INTO `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.transactions` AS target
USING `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.quality_checks` AS source
ON
  target.Transaction_Key = source.Transaction_Key
  AND target.is_current
    = TRUE
      WHEN
        MATCHED
        AND (
          target.SRC_TransactionID IS DISTINCT FROM source.SRC_TransactionID
          OR target.EncounterID IS DISTINCT FROM source.EncounterID
          OR target.PatientID IS DISTINCT FROM source.PatientID
          OR target.ProviderID IS DISTINCT FROM source.ProviderID
          OR target.DeptID IS DISTINCT FROM source.DeptID
          OR target.VisitDate IS DISTINCT FROM source.VisitDate
          OR target.ServiceDate IS DISTINCT FROM source.ServiceDate
          OR target.PaidDate IS DISTINCT FROM source.PaidDate
          OR target.VisitType IS DISTINCT FROM source.VisitType
          OR target.Amount IS DISTINCT FROM source.Amount
          OR target.AmountType IS DISTINCT FROM source.AmountType
          OR target.PaidAmount IS DISTINCT FROM source.PaidAmount
          OR target.ClaimID IS DISTINCT FROM source.ClaimID
          OR target.PayorID IS DISTINCT FROM source.PayorID
          OR target.ProcedureCode IS DISTINCT FROM source.ProcedureCode
          OR target.ICDCode IS DISTINCT FROM source.ICDCode
          OR target.LineOfBusiness IS DISTINCT FROM source.LineOfBusiness
          OR target.MedicaidID IS DISTINCT FROM source.MedicaidID
          OR target.MedicareID IS DISTINCT FROM source.MedicareID
          OR target.SRC_InsertDate IS DISTINCT FROM source.SRC_InsertDate
          OR target.SRC_ModifiedDate IS DISTINCT FROM source.SRC_ModifiedDate
          OR target.datasource IS DISTINCT FROM source.datasource
          OR target.is_quarantined IS DISTINCT FROM source.is_quarantined)
        THEN UPDATE
SET
  target.is_current = FALSE,
  target.modified_date = CURRENT_TIMESTAMP();

-- Step 2: insert new transactions AND new versions of changed ones
INSERT INTO `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.transactions`
  (
    Transaction_Key,
    SRC_TransactionID,
    EncounterID,
    PatientID,
    ProviderID,
    DeptID,
    VisitDate,
    ServiceDate,
    PaidDate,
    VisitType,
    Amount,
    AmountType,
    PaidAmount,
    ClaimID,
    PayorID,
    ProcedureCode,
    ICDCode,
    LineOfBusiness,
    MedicaidID,
    MedicareID,
    SRC_InsertDate,
    SRC_ModifiedDate,
    datasource,
    is_quarantined,
    inserted_date,
    modified_date,
    is_current)
SELECT
  source.Transaction_Key,
  source.SRC_TransactionID,
  source.EncounterID,
  source.PatientID,
  source.ProviderID,
  source.DeptID,
  source.VisitDate,
  source.ServiceDate,
  source.PaidDate,
  source.VisitType,
  source.Amount,
  source.AmountType,
  source.PaidAmount,
  source.ClaimID,
  source.PayorID,
  source.ProcedureCode,
  source.ICDCode,
  source.LineOfBusiness,
  source.MedicaidID,
  source.MedicareID,
  source.SRC_InsertDate,
  source.SRC_ModifiedDate,
  source.datasource,
  source.is_quarantined,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP(),
  TRUE
FROM `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.quality_checks` AS source
LEFT JOIN `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.transactions` AS target
  ON
    source.Transaction_Key = target.Transaction_Key AND target.is_current = TRUE
WHERE target.Transaction_Key IS NULL;

-- 4. DROP quality_check table (after both steps use it)
DROP TABLE IF EXISTS `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.quality_checks`;

-------------------------------------------------------------------------------------------------------
-- ENCOUNTERS
-------------------------------------------------------------------------------------------------------

-- 1. Create the encounters Table in BigQuery
CREATE TABLE IF NOT EXISTS `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.encounters`(
  Encounter_Key STRING,
  SRC_EncounterID STRING,
  PatientID STRING,
  ProviderID STRING,
  DepartmentID STRING,
  EncounterDate INT64,
  EncounterType STRING,
  ProcedureCode INT64,
  SRC_ModifiedDate INT64,
  datasource STRING,
  is_quarantined BOOL,
  inserted_date TIMESTAMP,
  modified_date TIMESTAMP,
  is_current BOOL);

-- 2. Create a quality_checks temp table for encounters
CREATE OR REPLACE TABLE `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.quality_checks_encounters`
AS
SELECT DISTINCT
  CONCAT(SRC_EncounterID, '-', datasource) AS Encounter_Key,
  SRC_EncounterID,
  PatientID,
  ProviderID,
  DepartmentID,
  EncounterDate,
  EncounterType,
  ProcedureCode,
  ModifiedDate AS SRC_ModifiedDate,
  datasource,
  CASE
    WHEN
      SRC_EncounterID IS NULL
      OR PatientID IS NULL
      OR EncounterDate IS NULL
      OR LOWER(EncounterType) = 'null'
      THEN TRUE
    ELSE FALSE
    END AS is_quarantined
FROM
  (
    SELECT DISTINCT
      EncounterID AS SRC_EncounterID,
      PatientID,
      ProviderID,
      DepartmentID,
      EncounterDate,
      EncounterType,
      ProcedureCode,
      ModifiedDate,
      'hospa' AS datasource
    FROM `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.encounters_ha`
    UNION ALL
    SELECT DISTINCT
      EncounterID AS SRC_EncounterID,
      PatientID,
      ProviderID,
      DepartmentID,
      EncounterDate,
      EncounterType,
      ProcedureCode,
      ModifiedDate,
      'hospb' AS datasource
    FROM `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.encounters_hb`
  );

-- 3. Apply SCD Type 2 Logic — Step 1: expire changed current records
MERGE INTO `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.encounters` AS target
USING
  `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.quality_checks_encounters`
    AS source
ON
  target.Encounter_Key = source.Encounter_Key
  AND target.is_current
    = TRUE
      WHEN
        MATCHED
        AND (
          target.SRC_EncounterID IS DISTINCT FROM source.SRC_EncounterID
          OR target.PatientID IS DISTINCT FROM source.PatientID
          OR target.ProviderID IS DISTINCT FROM source.ProviderID
          OR target.DepartmentID IS DISTINCT FROM source.DepartmentID
          OR target.EncounterDate IS DISTINCT FROM source.EncounterDate
          OR target.EncounterType IS DISTINCT FROM source.EncounterType
          OR target.ProcedureCode IS DISTINCT FROM source.ProcedureCode
          OR target.SRC_ModifiedDate IS DISTINCT FROM source.SRC_ModifiedDate
          OR target.datasource IS DISTINCT FROM source.datasource
          OR target.is_quarantined IS DISTINCT FROM source.is_quarantined)
        THEN UPDATE
SET
  target.is_current = FALSE,
  target.modified_date = CURRENT_TIMESTAMP();

-- Step 2: insert new encounters AND new versions of changed ones
INSERT INTO `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.encounters`
  (
    Encounter_Key,
    SRC_EncounterID,
    PatientID,
    ProviderID,
    DepartmentID,
    EncounterDate,
    EncounterType,
    ProcedureCode,
    SRC_ModifiedDate,
    datasource,
    is_quarantined,
    inserted_date,
    modified_date,
    is_current)
SELECT
  source.Encounter_Key,
  source.SRC_EncounterID,
  source.PatientID,
  source.ProviderID,
  source.DepartmentID,
  source.EncounterDate,
  source.EncounterType,
  source.ProcedureCode,
  source.SRC_ModifiedDate,
  source.datasource,
  source.is_quarantined,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP(),
  TRUE
FROM
  `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.quality_checks_encounters`
    AS source
LEFT JOIN `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.encounters` AS target
  ON source.Encounter_Key = target.Encounter_Key AND target.is_current = TRUE
WHERE target.Encounter_Key IS NULL;

-- 4. DROP quality_check table
DROP TABLE IF EXISTS `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.quality_checks_encounters`;

-------------------------------------------------------------------------------------------------------
-- CLAIMS
-------------------------------------------------------------------------------------------------------

-- 1. Create the Claims Table in BigQuery
CREATE TABLE IF NOT EXISTS `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.claims`(
  Claim_Key STRING,
  SRC_ClaimID STRING,
  TransactionID STRING,
  PatientID STRING,
  EncounterID STRING,
  ProviderID STRING,
  DeptID STRING,
  ServiceDate STRING,
  ClaimDate STRING,
  PayorID STRING,
  ClaimAmount STRING,
  PaidAmount STRING,
  ClaimStatus STRING,
  PayorType STRING,
  Deductible STRING,
  Coinsurance STRING,
  Copay STRING,
  SRC_InsertDate STRING,
  SRC_ModifiedDate STRING,
  datasource STRING,
  is_quarantined BOOLEAN,
  inserted_date TIMESTAMP,
  modified_date TIMESTAMP,
  is_current BOOLEAN);

-- 2. Create a quality_checks temp table for claims
-- NOTE: bronze_dataset.claims already has a real datasource column (hospa/hospb) — read it, don't hardcode
CREATE OR REPLACE TABLE `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.quality_checks_claims`
AS
SELECT
  CONCAT(SRC_ClaimID, '-', datasource) AS Claim_Key,
  SRC_ClaimID,
  TransactionID,
  PatientID,
  EncounterID,
  ProviderID,
  DeptID,
  ServiceDate,
  ClaimDate,
  PayorID,
  ClaimAmount,
  PaidAmount,
  ClaimStatus,
  PayorType,
  Deductible,
  Coinsurance,
  Copay,
  InsertDate AS SRC_InsertDate,
  ModifiedDate AS SRC_ModifiedDate,
  datasource,
  CASE
    WHEN
      SRC_ClaimID IS NULL
      OR PatientID IS NULL
      OR TransactionID IS NULL
      OR LOWER(ClaimStatus) = 'null'
      THEN TRUE
    ELSE FALSE
    END AS is_quarantined
FROM
  (
    SELECT
      ClaimID AS SRC_ClaimID,
      TransactionID,
      PatientID,
      EncounterID,
      ProviderID,
      DeptID,
      ServiceDate,
      ClaimDate,
      PayorID,
      ClaimAmount,
      PaidAmount,
      ClaimStatus,
      PayorType,
      Deductible,
      Coinsurance,
      Copay,
      InsertDate,
      ModifiedDate,
      datasource
    FROM `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.claims`
  );

-- 3. Apply SCD Type 2 Logic — Step 1: expire changed current records
MERGE INTO `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.claims` AS target
USING
  `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.quality_checks_claims`
    AS source
ON
  target.Claim_Key = source.Claim_Key
  AND target.is_current
    = TRUE
      WHEN
        MATCHED
        AND (
          target.SRC_ClaimID IS DISTINCT FROM source.SRC_ClaimID
          OR target.TransactionID IS DISTINCT FROM source.TransactionID
          OR target.PatientID IS DISTINCT FROM source.PatientID
          OR target.EncounterID IS DISTINCT FROM source.EncounterID
          OR target.ProviderID IS DISTINCT FROM source.ProviderID
          OR target.DeptID IS DISTINCT FROM source.DeptID
          OR target.ServiceDate IS DISTINCT FROM source.ServiceDate
          OR target.ClaimDate IS DISTINCT FROM source.ClaimDate
          OR target.PayorID IS DISTINCT FROM source.PayorID
          OR target.ClaimAmount IS DISTINCT FROM source.ClaimAmount
          OR target.PaidAmount IS DISTINCT FROM source.PaidAmount
          OR target.ClaimStatus IS DISTINCT FROM source.ClaimStatus
          OR target.PayorType IS DISTINCT FROM source.PayorType
          OR target.Deductible IS DISTINCT FROM source.Deductible
          OR target.Coinsurance IS DISTINCT FROM source.Coinsurance
          OR target.Copay IS DISTINCT FROM source.Copay
          OR target.SRC_ModifiedDate IS DISTINCT FROM source.SRC_ModifiedDate
          OR target.datasource IS DISTINCT FROM source.datasource
          OR target.is_quarantined IS DISTINCT FROM source.is_quarantined)
        THEN UPDATE
SET
  target.is_current = FALSE,
  target.modified_date = CURRENT_TIMESTAMP();

-- Step 2: insert new claims AND new versions of changed ones
INSERT INTO `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.claims`
  (
    Claim_Key,
    SRC_ClaimID,
    TransactionID,
    PatientID,
    EncounterID,
    ProviderID,
    DeptID,
    ServiceDate,
    ClaimDate,
    PayorID,
    ClaimAmount,
    PaidAmount,
    ClaimStatus,
    PayorType,
    Deductible,
    Coinsurance,
    Copay,
    SRC_InsertDate,
    SRC_ModifiedDate,
    datasource,
    is_quarantined,
    inserted_date,
    modified_date,
    is_current)
SELECT
  source.Claim_Key,
  source.SRC_ClaimID,
  source.TransactionID,
  source.PatientID,
  source.EncounterID,
  source.ProviderID,
  source.DeptID,
  source.ServiceDate,
  source.ClaimDate,
  source.PayorID,
  source.ClaimAmount,
  source.PaidAmount,
  source.ClaimStatus,
  source.PayorType,
  source.Deductible,
  source.Coinsurance,
  source.Copay,
  source.SRC_InsertDate,
  source.SRC_ModifiedDate,
  source.datasource,
  source.is_quarantined,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP(),
  TRUE
FROM
  `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.quality_checks_claims`
    AS source
LEFT JOIN `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.claims` AS target
  ON source.Claim_Key = target.Claim_Key AND target.is_current = TRUE
WHERE target.Claim_Key IS NULL;

-- 4. DROP quality_check table
DROP TABLE IF EXISTS `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.quality_checks_claims`;
