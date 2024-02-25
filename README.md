# Bhargava Reddy's Assignment - PEI

This repository contains the assignment solution for PEI.

## Table of Contents

- [Getting Started](#getting-started)
- [Usage](#usage)


## Getting Started

To get started with this project, you can load the repository directly into Databricks using Databricks Repos. Follow these steps:
1. **Prerequisitess**: Ensure that all required files are uploaded to Databricks DBFS.
2. **Databricks Environments**: Note that all notebooks are intended to run in Databricks since Spark initialization happens automatically in Databricks.
3. **Databricks Repos**: Open your Databricks workspace and navigate to the Repos tab.
4. **Import Repository**: Click on "Import Repo" and provide the URL of this GitHub repository (`https://github.com/bhargav066/bhargava_reddy_assignment_pei.git`).
5. **Clone Repository**: After importing, clone the repository into your Databricks workspace.

## Usage

Once the repository is imported into your Databricks workspace, follow these steps to run the notebooks:

1. **Data Ingestion**: Run the `data_ingestion.py` notebook first. This notebook ingests required raw tables from different file formats.
2. **Data Transformation and Load**: After ingesting raw tables, run the `data_transformation_and_load.py` notebook to process the acquired data and load the desired tables and SQL outputs.
3. **Data Quality Checks**: For data checks and unit tests, run the `data_quality_checks.py` and `tests.py` notebooks separately.
