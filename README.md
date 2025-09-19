# Data-Warehouse
This project is designed to build a data warehouse. This data warehouse will enable organizations to store, manage, and analyze large datasets in a cost-effective, secure, and scalable manner. The data warehouse will provide a centralized repository for all data, allowing users to easily access and query the data.
Using an orchestration system by Dagster to build ETL pipeline inlcude three steps: Extract, Transform and Load. Raw file is CSV then extract to DuckDB by Python, then transform by Pandas and last step is to load to PostgreSQL. The final data can be used for reporting, analysis.
# System Architecture
<img width="972" height="454" alt="image" src="https://github.com/user-attachments/assets/7d2ac880-6ef0-4217-ae3b-962dba7999d6" /> <br />
# Data Layer <br />
<img width="958" height="399" alt="image" src="https://github.com/user-attachments/assets/459eab80-bea4-4f3e-9087-8ad3c50934e6" /> <br />
Bronze Layer: This is where raw data from CSV files is ingested. It contains the purest form of data, taken directly from the company’s database./> <br />
Silver Layer: After undergoing cleaning, transformation, and validation, the data moves to the Silver Layer. Here, it is integrated with SCD - Type 2 to track historical changes./> <br />
Gold Layer: This is the final stage, where the data is grouped and modeled following a Star Schema, filtering out the most valuable insights for the business.

