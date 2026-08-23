CREATE EXTERNAL TABLE IF NOT EXISTS  `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.departments_ha` 
OPTIONS (
  FORMAT = 'json',
  URIS = ['gs://healthcare-bucket-8826/landing/hospital-a/departments/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS  `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.encounters_ha` 
OPTIONS (
  FORMAT = 'json',
  URIS = ['gs://healthcare-bucket-8826/landing/hospital-a/encounters/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS  `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.patients_ha` 
OPTIONS (
  FORMAT = 'json',
  URIS = ['gs://healthcare-bucket-8826/landing/hospital-a/patients/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS  `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.providers_ha` 
OPTIONS (
  FORMAT = 'json',
  URIS = ['gs://healthcare-bucket-8826/landing/hospital-a/providers/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS  `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.transcations_ha` 
OPTIONS (
  FORMAT = 'json',
  URIS = ['gs://healthcare-bucket-8826/landing/hospital-a/transactions/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS  `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.departments_hb` 
OPTIONS (
  FORMAT = 'json',
  URIS = ['gs://healthcare-bucket-8826/landing/hospital-b/departments/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS  `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.encounters_hb` 
OPTIONS (
  FORMAT = 'json',
  URIS = ['gs://healthcare-bucket-8826/landing/hospital-b/encounters/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS  `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.patients_hb` 
OPTIONS (
  FORMAT = 'json',
  URIS = ['gs://healthcare-bucket-8826/landing/hospital-b/patients/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS  `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.providers_hb` 
OPTIONS (
  FORMAT = 'json',
  URIS = ['gs://healthcare-bucket-8826/landing/hospital-b/providers/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS  `project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.transcations_hb` 
OPTIONS (
  FORMAT = 'json',
  URIS = ['gs://healthcare-bucket-8826/landing/hospital-b/transactions/*.json']
);