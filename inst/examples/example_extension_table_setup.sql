-- Example SQL scripts for creating extension tables
-- These tables extend the OMOP CDM with custom data elements

-- ============================================================================
-- Example 1: Patient Biomarkers Extension Table
-- ============================================================================

-- Create the biomarker data table
CREATE TABLE my_extensions.patient_biomarkers (
  person_id BIGINT NOT NULL,
  biomarker_id INT NOT NULL,
  biomarker_value FLOAT,
  biomarker_unit VARCHAR(50),
  measurement_date DATE NOT NULL,
  created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (person_id, biomarker_id, measurement_date)
);

-- Create the biomarker definitions reference table
CREATE TABLE my_extensions.biomarker_definitions (
  biomarker_id INT PRIMARY KEY,
  biomarker_name VARCHAR(255) NOT NULL,
  biomarker_description TEXT,
  standard_unit VARCHAR(50),
  normal_range_low FLOAT,
  normal_range_high FLOAT
);

-- Create indexes for performance
CREATE INDEX idx_biomarker_person ON my_extensions.patient_biomarkers(person_id);
CREATE INDEX idx_biomarker_date ON my_extensions.patient_biomarkers(measurement_date);
CREATE INDEX idx_biomarker_id ON my_extensions.patient_biomarkers(biomarker_id);

-- Insert sample biomarker definitions
INSERT INTO my_extensions.biomarker_definitions
  (biomarker_id, biomarker_name, biomarker_description, standard_unit, normal_range_low, normal_range_high)
VALUES
  (1, 'Hemoglobin A1c', 'Glycated hemoglobin test for diabetes monitoring', '%', 4.0, 5.6),
  (2, 'LDL Cholesterol', 'Low-density lipoprotein cholesterol', 'mg/dL', 0, 100),
  (3, 'HDL Cholesterol', 'High-density lipoprotein cholesterol', 'mg/dL', 40, 200),
  (4, 'Triglycerides', 'Serum triglyceride level', 'mg/dL', 0, 150),
  (5, 'C-Reactive Protein', 'Inflammation marker', 'mg/L', 0, 3.0);

-- Insert sample data
INSERT INTO my_extensions.patient_biomarkers (person_id, biomarker_id, biomarker_value, biomarker_unit, measurement_date)
VALUES
  (1001, 1, 6.5, '%', '2024-01-15'),
  (1001, 2, 130, 'mg/dL', '2024-01-15'),
  (1002, 1, 5.2, '%', '2024-02-20'),
  (1002, 3, 55, 'mg/dL', '2024-02-20');
