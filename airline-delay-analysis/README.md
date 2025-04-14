# ✈️ US Domestic Airline Delay Data Analysis

This project analyzes flight delay patterns using a dataset from August 2018 to identify operational inefficiencies in the US domestic airline industry. We used **MongoDB**, **Snowflake**, and **Tableau** to uncover insights on delays, cancellations, and performance patterns.

---
## 📂 Dataset

The dataset used for this project was sourced from the U.S. Department of Transportation, made publicly available via Data.World:

🔗 [Airline On-Time Performance Statistics Dataset](https://data.world/dot/airline-on-time-performance-statistics)

It includes flight, airport, and delay information for U.S. domestic flights in August 2018.

---
## 🧠 Objective

To investigate the causes of airline delays and cancellations and build a scalable analytics solution using:
- Cloud-based database systems (MongoDB Atlas, Snowflake)
- SQL and aggregation pipelines
- Data visualization tools (Tableau)

---

## 🛠️ Tech Stack

| Component         | Tool / Platform                      |
|------------------|--------------------------------------|
| NoSQL DB         | MongoDB Atlas (with Sharding)        |
| Relational DB     | Snowflake                            |
| Visualization     | Tableau                              |
| Data Analysis     | Python, Pandas, NumPy                |
| CLI               | MongoShell                           |

---

## 🔄 Architecture & Workflow

1. **MongoDB Atlas**:
   - Hosted and sharded the dataset
   - Connected using MongoDB Compass and MongoShell
   - Imported CSV via `mongoimport`
   - Used aggregation pipeline for complex queries

2. **Snowflake**:
   - Designed a normalized schema with 4 tables:
     - `Airlines`, `Airports`, `Flights`, `Flight_Delays`
   - Ran SQL queries for delay pattern analysis

3. **Tableau**:
   - Connected to Snowflake database
   - Built dashboards for visualizing delays, cancellations, peak timings

4. **Python**:
   - Used Pandas for data cleaning and transformation
   - Created new features (e.g., actual elapsed time, delay categories)

---

## 📊 Business Use Cases

| Use Case | Description |
|----------|-------------|
| 1 | Total number of flights per airline |
| 2 | Average departure delay by airline |
| 3 | Cancellation count and reasons |
| 4 | Top 5 busiest airports |
| 5 | Delay distribution (early, on-time, severe delay) |
| 6 | Average delay by day of the week |
| 7 | Airline with highest weather delay |
| 8 | Busiest day of the week |
| 9 | On-time vs delayed vs canceled flights by route |
| 10 | Peak departure hours |

---

## 📈 Results

- Identified key delay contributors: **Air Carrier** and **NAS**
- Visualized daily and hourly flight patterns
- Derived KPIs like average delay time and cancellation rates
- Built a scalable pipeline across **MongoDB**, **Snowflake**, and **Tableau**

---


