-------------------------------------------------------------------------------------------------------
-- HOSPITAL A
-------------------------------------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.departments_ha`
(
  DeptID STRING,
  Name STRING
)
OPTIONS (
  FORMAT = 'json',
  URIS = ['gs://healthcare-bucket-8826/landing/hospital-a/departments/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.encounters_ha`
(
  EncounterID STRING,
  PatientID STRING,
  EncounterDate INT64,
  EncounterType STRING,
  ProviderID STRING,
  DepartmentID STRING,
  ProcedureCode INT64,
  InsertedDate INT64,
  ModifiedDate INT64
)
OPTIONS (
  FORMAT = 'json',
  URIS = ['gs://healthcare-bucket-8826/landing/hospital-a/encounters/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.patients_ha`
(
  PatientID STRING,
  FirstName STRING,
  LastName STRING,
  MiddleName STRING,
  SSN STRING,
  PhoneNumber STRING,
  Gender STRING,
  DOB INT64,
  Address STRING,
  ModifiedDate INT64
)
OPTIONS (
  FORMAT = 'json',
  URIS = ['gs://healthcare-bucket-8826/landing/hospital-a/patients/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.providers_ha`
(
  ProviderID STRING,
  FirstName STRING,
  LastName STRING,
  Specialization STRING,
  DeptID STRING,
  NPI INT64
)
OPTIONS (
  FORMAT = 'json',
  URIS = ['gs://healthcare-bucket-8826/landing/hospital-a/providers/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.transcations_ha`
(
  TransactionID STRING,
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
  InsertDate INT64,
  ModifiedDate INT64
)
OPTIONS (
  FORMAT = 'json',
  URIS = ['gs://healthcare-bucket-8826/landing/hospital-a/transactions/*.json']
);

-------------------------------------------------------------------------------------------------------
-- HOSPITAL B
-------------------------------------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.departments_hb`
(
  DeptID STRING,
  Name STRING
)
OPTIONS (
  FORMAT = 'json',
  URIS = ['gs://healthcare-bucket-8826/landing/hospital-b/departments/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.encounters_hb`
(
  EncounterID STRING,
  PatientID STRING,
  EncounterDate INT64,
  EncounterType STRING,
  ProviderID STRING,
  DepartmentID STRING,
  ProcedureCode INT64,
  InsertedDate INT64,
  ModifiedDate INT64
)
OPTIONS (
  FORMAT = 'json',
  URIS = ['gs://healthcare-bucket-8826/landing/hospital-b/encounters/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.patients_hb`
(
  ID STRING,
  F_Name STRING,
  L_Name STRING,
  M_Name STRING,
  SSN STRING,
  PhoneNumber STRING,
  Gender STRING,
  DOB INT64,
  Address STRING,
  ModifiedDate INT64
)
OPTIONS (
  FORMAT = 'json',
  URIS = ['gs://healthcare-bucket-8826/landing/hospital-b/patients/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.providers_hb`
(
  ProviderID STRING,
  FirstName STRING,
  LastName STRING,
  Specialization STRING,
  DeptID STRING,
  NPI INT64
)
OPTIONS (
  FORMAT = 'json',
  URIS = ['gs://healthcare-bucket-8826/landing/hospital-b/providers/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.transcations_hb`
(
  TransactionID STRING,
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
  InsertDate INT64,
  ModifiedDate INT64
)
OPTIONS (
  FORMAT = 'json',
  URIS = ['gs://healthcare-bucket-8826/landing/hospital-b/transactions/*.json']
);