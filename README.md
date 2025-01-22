# DE_Dmart-Analysis-using-Pyspark


## Overview
This project performs data analysis on a retail dataset using PySpark. It integrates PySpark and Streamlit to process and visualize data from three CSV files: Products, Customers, and Sales. The goal is to gain insights through a series of analytical questions and queries.

---

## Prerequisites
Ensure you have the following installed:
1. Python (>= 3.8)
2. Apache Spark (PySpark)
3. Streamlit

---

## Files Used

### Input Data Files
- **Product.csv**: Contains product details.
- **Customer.csv**: Contains customer details.
- **Sales.csv**: Contains sales transaction details.

### Output
Data insights and visualizations are rendered in the Streamlit application.

---

## Key Features

### Data Loading and Cleaning
- Load CSV files into PySpark DataFrames.
- Drop rows with missing values.
- Rename columns for consistency.

### Data Processing
- Join Product, Customer, and Sales datasets on relevant keys.
- Perform aggregations and transformations for analysis.

### Analytical Tasks
1. **Count the number of products in each category.**
2. **Find the average sales for each product.**
3. **Find the maximum discount given on any product.**
4. **Find the maximum discount for each product category.**
5. **Calculate the average discount for each product category.**
6. **Compute the total quantity of products sold in each city.**
7. **Count the number of customers in each region.**
8. **Determine the total profit for each product category.**
9. **Find the total sales amount for each product sub-category.**
10. **Determine the number of orders placed by each customer.**

### Querying Capabilities
- Identify the customer with the highest number of purchases.
- Calculate the average discount given across all products.
- Count the unique products sold in each region.
- Determine the total profit generated in each state.
- Find the product sub-category with the highest sales.
- Compute the average age of customers in each segment.
- Count the number of orders shipped by each shipping mode.
- Calculate the total quantity of products sold in each city.
- Identify the customer segment with the highest profit margin.

---

## Running the Application

### Step 1: Set up the Environment
1. Install dependencies using the following command:
   ```bash
   pip install pyspark streamlit pandas
   ```

2. Replace the file paths for `Product.csv`, `Customer.csv`, and `Sales.csv` in the script with the correct paths on your system.

### Step 2: Start the Application
Run the Streamlit application:
```bash
streamlit run app.py
```

### Step 3: View the Results
The application will launch in your default browser. You can interact with the analysis and view the results.

---

## Example Insights
1. **Highest Sales Sub-Category**: Identify the sub-category with the highest total sales.
2. **Most Profitable Segment**: Determine which customer segment yields the highest profit margin.
3. **Customer Orders**: Analyze the order count for each customer.

---



