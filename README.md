# Real-Time Data Cleaning & Feature Engineering Pipeline

## Project Overview
[cite_start]This project, for the Data Mining course at Universidad Politécnica de Yucatán [cite: 1][cite_start], demonstrates an end-to-end real-time data preprocessing system using Apache Kafka and MongoDB[cite: 6]. [cite_start]The core of the project is a pipeline that consumes intentionally "dirty" data streams, applies robust cleaning and validation rules, transforms and integrates the data, and engineers new features for analysis[cite: 7].

## System Architecture
The pipeline consists of four main Docker services:
1.  [cite_start]**Kafka**: The message broker that handles the data streams[cite: 99].
2.  [cite_start]**MongoDB**: The database for storing raw, cleaned, curated, and feature-enriched data[cite: 101, 102].
3.  [cite_start]**Dirty Producer**: A custom Python script that generates noisy data for three Kafka topics (`dirty_events`, `dirty_metadata`, `dirty_reference`) to simulate real-world data quality issues[cite: 104, 108].
4.  [cite_start]**Cleaning Pipeline**: A Python consumer service that performs all cleaning, transformation, and feature engineering logic[cite: 105].

## Data Quality Issues Injected
The producer intentionally introduces the following issues into the data stream:
- [cite_start]**Missing Fields**: Null values for timestamps and other fields[cite: 34].
- [cite_start]**Inconsistent Casing**: Categorical data like `platform` appears as 'Web', 'web', etc.[cite: 35].
- [cite_start]**Duplicated Events**: The same `event_id` is sent multiple times[cite: 36].
- [cite_start]**Corrupted Data Types**: Numeric fields like `value` are sent as strings (e.g., "twenty")[cite: 37].
- [cite_start]**Mixed Timestamp Formats**: Timestamps are sent as ISO strings, Unix epochs, and custom string formats[cite: 38].

## [cite_start]Cleaning Rules and Assumptions [cite: 109]
The cleaning pipeline implements the following logic:
- **Timestamp Standardization**: All valid timestamps are converted to ISO 8601 format. [cite_start]Unparseable timestamps are set to `null`[cite: 48].
- **Type Enforcement**: Values are converted to their correct numeric types. [cite_start]Non-numeric values are converted to `null`[cite: 49].
- [cite_start]**Duplicate Removal**: Duplicate events are detected and dropped using a unique index on `event_id` in MongoDB[cite: 50].
- [cite_start]**Normalization**: String fields are converted to lowercase to ensure consistency[cite: 52].
- **Imputation**: Missing values are currently flagged or set to `null`. No complex imputation is performed.

## [cite_start]Transformation and Feature Engineering Logic [cite: 110]
- **Integration**: Cleaned data streams are merged based on `entity_id` using a MongoDB aggregation pipeline.
- **Calculated Fields**: `value_delta`, `z_score`, and `is_outlier` are calculated to provide context.
- **Engineered Features**: Five new features, including rolling means and rate of change, are generated and stored in a separate collection. See `reports/features_dictionary.md` for details.

## [cite_start]How to Run the Pipeline [cite: 113]
1.  **Prerequisites**: Docker and Docker Compose must be installed.
2.  **Build and Run**: From the root of the project directory, run the following command:
    ```bash
    docker-compose up --build
    ```
3.  **View Data**: Connect to MongoDB at `mongodb://localhost:27017/` and inspect the following collections: `raw_dirty`, `cleaned.events`, `curated.events`, and `curated.features`.
4.  **Run Analysis**: To run the exploratory analysis, ensure you have a Jupyter environment. Execute the cells in `notebooks/exploration.ipynb`.

## [cite_start]Data Quality Improvement Summary [cite: 111, 114]
The cleaning pipeline significantly improves data quality. Below is a comparison of the `value` distribution and `platform` categories before and after processing.

**Value Distribution:**
![Value Distribution Comparison](./reports/visuals/distribution_comparison.png)

**Platform Standardization:**
![Platform Standardization](./reports/visuals/platform_standardization.png)
