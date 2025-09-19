# Data-Warehouse
This project is designed to build a data warehouse. This data warehouse will enable organizations to store, manage, and analyze large datasets in a cost-effective, secure, and scalable manner. The data warehouse will provide a centralized repository for all data, allowing users to easily access and query the data.
Using an orchestration system by Dagster to build ETL pipeline inlcude three steps: Extract, Transform and Load. Raw file is CSV then extract to DuckDB by Python, then transform by Pandas and last step is to load to PostgreSQL. The final data can be used for reporting, analysis.
# System Architecture
<img width="972" height="454" alt="image" src="https://github.com/user-attachments/assets/7d2ac880-6ef0-4217-ae3b-962dba7999d6" /> <br />
# Data Layer <br />
<img width="958" height="399" alt="image" src="https://github.com/user-attachments/assets/459eab80-bea4-4f3e-9087-8ad3c50934e6" /> <br />
𝗕𝗿𝗼𝗻𝘇𝗲 𝗟𝗮𝘆𝗲𝗿: Đây là nơi nhận vào dữ liệu thô từ file csv. Dữ liệu thuần túy nhất, được lấy trực tiếp từ database của doanh nghiệp <br />
𝗦𝗶𝗹𝘃𝗲𝗿 𝗟𝗮𝘆𝗲𝗿: Sau khi trải qua quá trình clean, transform và validation, dữ liệu sẽ được chuyển đến Lớp Bạc. Tại đây, dữ liệu kết hợp với SCD - Type 2, nhằm theo dõi lịch sử dữ liệu. <br />
𝗚𝗼𝗹𝗱 𝗟𝗮𝘆𝗲𝗿: Đây là giai đoạn cuối cùng, nơi dữ liệu được group, modelling theo Star schema nhằm sàn lọc ra dữ liệu có ích nhất cho doanh nghiệp. <br />

