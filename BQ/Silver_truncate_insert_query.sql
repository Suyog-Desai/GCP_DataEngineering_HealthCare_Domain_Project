--  IMPLEMENTING BOTH SCD2 AND CDM LOGIC FOR THE SILVER TABLES

-- 1. Create table departments by Merge Data from Hospital A & B

CREATE TABLE IF NOT EXISTS `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.departments`(
  Dept_Id STRING,
  SRC_Dept_Id STRING,
  Name STRING,
  datasource STRING,
  is_quarantined BOOLEAN);

-- 2. Truncate Silver Table Before Inserting
TRUNCATE TABLE `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.departments`;

-- 3. full load by Inserting merged Data
INSERT INTO `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.departments`
SELECT DISTINCT
  CONCAT(deptid, '-', datasource) AS Dept_Id,
  DEptID AS src_dept_id,
  Name,
  datasource,
  CASE
    WHEN DEptID IS NULL OR Name IS NULL THEN TRUE
    ELSE FALSE
    END AS is_quarantined
FROM
  (
    SELECT DISTINCT *, 'hospa' AS datasource
    FROM `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.departments_ha`
    UNION ALL
    SELECT DISTINCT *, 'hospb' AS datasource
    FROM `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.departments_hb`
  );

-- 1. Create table providers by Merge Data from Hospital A & B
CREATE TABLE IF NOT EXISTS `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.providers`(
  ProviderID STRING,
  FirstName STRING,
  LastName STRING,
  Specialization STRING,
  DeptID STRING,
  NPI INT64,
  datasource STRING,
  is_quarantined BOOLEAN);

-- 2. Truncate Silver Table Before Inserting
TRUNCATE TABLE `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.providers`;

-- 3. full load by Inserting merged Data
INSERT INTO `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.providers`
SELECT DISTINCT
  ProviderID,
  FirstName,
  LastName,
  Specialization,
  DeptID,
  CAST(NPI AS INT64) AS NPI,
  datasource,
  CASE
    WHEN ProviderID IS NULL OR DeptID IS NULL THEN TRUE
    ELSE FALSE
    END AS is_quarantined
FROM
  (
    SELECT DISTINCT *, 'hosa' AS datasource
    FROM `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.providers_ha`
    UNION ALL
    SELECT DISTINCT *, 'hosb' AS datasource
    FROM `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.providers_ha`
  );

  -- CDM LOGIC FOR CPT_CODES (universal reference table — no datasource, no SCD2 history needed)

-- 1. Create cpt_codes table
CREATE TABLE IF NOT EXISTS `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.cpt_codes` (
    CP_Code_Key STRING,
    SRC_CPT_Code STRING,
    procedure_code_category STRING,
    procedure_code_descriptions STRING,
    code_status STRING,
    is_quarantined BOOLEAN
);

-- 2. Truncate Silver Table Before Inserting
TRUNCATE TABLE `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.cpt_codes`;

-- 3. Full load by inserting data
INSERT INTO `project-66ab2fa5-e082-4e6e-9c4.silver_dataset.cpt_codes`
SELECT DISTINCT
    cpt_codes AS CP_Code_Key,
    cpt_codes AS SRC_CPT_Code,
    procedure_code_category,
    procedure_code_descriptions,
    code_status,
    CASE
        WHEN cpt_codes IS NULL OR LOWER(code_status) = 'null' THEN TRUE
        ELSE FALSE
    END AS is_quarantined
FROM `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.cpt_codes`;
