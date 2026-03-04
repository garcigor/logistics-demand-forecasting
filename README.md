# 🚚 Logistics Demand Forecasting

This project is an end-to-end Data Engineering and Data Science pipeline built to automate the cleaning, transformation, and demand forecasting of logistics products using Artificial Intelligence.

## 🏗️ Architecture and Technologies

* **Orchestration & CI/CD:** GitHub Actions with automated daily scheduling (Cron jobs).
* **Security:** Workload Identity Federation (WIF) for secure, keyless Google Cloud authentication.
* **Data Transformation:** dbt (Data Build Tool) utilizing the Medallion Architecture (Silver and Gold layers).
* **Data Warehouse:** Google BigQuery.
* **Machine Learning:** BigQuery ML (Training an `ARIMA_PLUS` time-series forecasting model directly via SQL).
* **Data Visualization (BI):** Looker Studio.

## ⚙️ How the Pipeline Works

1. **Data Generation:** The `gerar_big_data.py` script generates a massive historical sales dataset (`vendas_vasto.parquet`) to simulate real-world volume.
2. **Data Engineering (Silver/Gold):** dbt cleans the raw data and engineers forecasting features (such as 7-day moving averages) into the Gold layer.
3. **Machine Learning:** The ARIMA model analyzes seasonality and trends from the Gold layer to predict the demand for the next 30 days for each individual SKU.
4. **Dashboard:** The forecasted data automatically feeds into a dynamic visualization panel to support decision-making.

## 📊 Results (Forecasting Dashboard)
<img width="994" height="733" alt="Captura de tela 2026-03-04 194933" src="https://github.com/user-attachments/assets/6c6e70eb-5d21-4ab0-b1d4-e08a7d647e14" />
