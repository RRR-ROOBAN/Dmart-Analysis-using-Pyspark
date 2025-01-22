
import pandas as pd

from pyspark.sql import SparkSession

from pyspark import SparkContext

import streamlit as st

from pyspark.sql.functions import *


#Initialize Spark

spark=SparkSession.builder.appName("Read csv file").getOrCreate()

#csv file path

product_file="E:\Guvi Project\Dmart analysis using pyspark\Inputdata\Product.csv"
customer_file="E:\Guvi Project\Dmart analysis using pyspark\Inputdata\Customer.csv"
sales_file="E:\Guvi Project\Dmart analysis using pyspark\Inputdata\Sales.csv"

#Load Dataframe

product_dt=spark.read.option("header","true").option("inferschema","true").csv(product_file)
customer_dt=spark.read.option("header","true").option("inferschema","true").csv(customer_file)
sales_dt=spark.read.option("header","true").option("inferschema","true").csv(sales_file)
"""
product_dt.show()

customer_dt.show()

sales_dt.show()

product_dt.columns

sales_dt.columns

customer_dt.columns"""

#Rename Column for Consistency

customer_dt=customer_dt.withColumnRenamed("Customer ID","Customer_ID")
sales_dt=sales_dt.withColumnRenamed("Customer ID","Customer_ID")
sales_dt=sales_dt.withColumnRenamed("Product ID","Product_ID")
product_dt=product_dt.withColumnRenamed("Product ID","Product_ID")

#Handle missing values appropriately.

customer_dt.dropna(how="any")
sales_dt.dropna(how="any")
product_dt.dropna(how="any")
"""
Ensure data types

product_dt.printSchema()
sales_dt.printSchema()
customer_dt.printSchema()"""

#Join the DataFrames

# Join sales_dt with product_dt on Product_ID
sales_product_df = sales_dt.join(product_dt, on="Product_ID", how="inner")

# Join the result with customer_dt on Customer_ID
final_df = sales_product_df.join(customer_dt, on="Customer_ID", how="inner")





# Show the resulting DataFrame
#final_df.show()

st.title("DE_Dmart-Analysis-using-Pyspark")
#10 Analytical Questions

#final_df.columns
st.write(" Data Analysis and Querying")

#1.Count the number of products in each category.

st.write ("1.Count the number of products in each category.")

task1=final_df.groupby("Category").agg({"Product_ID":"count"}).withColumnRenamed("count(Product_ID)","Count")
task1_pandas=task1.toPandas()
st.dataframe(task1_pandas)


#2.Find the average sales for each product.

st.write ("2.Find the average sales for each product.")

task2=final_df.groupby("Product Name").agg({"Sales":"Avg"}).withColumnRenamed("Avg(Sales)","Average")
task2_pandas=task2.toPandas()
st.dataframe(task2)

#3.Find the maximum discount given on any product.


st.write ("3.Find the maximum discount given on any product.")

task3=final_df.agg({"Discount": "max"})
task3_pandas=task3.toPandas()
st.dataframe(task3)

#4.What is the max discount given to each product category.

st.write ("4.What is the max discount given to each product category.")

task4 = final_df.groupby("Sub-Category").agg(max("Discount").alias("Max_Discount"))
task4_pandas=task4.toPandas()
st.dataframe(task4)

#5.What is the average discount given to each product category.

st.write ("5.What is the average discount given to each product category.")

task5 = final_df.groupBy("Category").agg({"Discount": "avg"}).withColumnRenamed("avg(Discount)", "Avg_Discount")
task5_pandas=task5.toPandas()
st.dataframe(task5)

#6.Calculate the total quantity of products sold in each city.

st.write ("6.Calculate the total quantity of products sold in each city.")

task6=final_df.groupby("City").agg({"Quantity":"sum"}).withColumnRenamed("sum(Quantity)","Total Quantity")
task6_pandas=task6.toPandas()
st.dataframe(task6)

#7.Count the number of customers in each region.

st.write ("7.Count the number of customers in each region.")


task7=final_df.groupby("Region").agg(countDistinct("Customer_ID").alias("Count"))
task7_pandas=task7.toPandas()
st.dataframe(task7)

#8.What is the total profit for each product category.

st.write ("8.What is the total profit for each product category.")

task8=final_df.groupby("Category").agg({"Profit":"sum"}).withColumnRenamed("sum(Profit)","Total Profit")
task8_pandas=task8.toPandas()
st.dataframe(task8)

#9.What is the total sales amount for each product sub-category

st.write ("9.What is the total sales amount for each product sub-category")

task9=final_df.groupby("Sub-Category").agg({"Sales":"sum"}).withColumnRenamed("sum(Sales)","Total Sales")
task9_pandas=task9.toPandas()
st.dataframe(task9)

#10.What is the number of orders placed by each customer

st.write ("10.What is the number of orders placed by each customer")

task10=final_df.groupby("Customer_ID","Customer Name").agg({"Order ID":"count"}).withColumnRenamed("count(Order ID)","Count")
task10_pandas=task10.toPandas()
st.dataframe(task10)



st.write("Run Queries on the Pyspark")


#Q1.Which customer has made the highest number of purchases

st.write ("Q1.Which customer has made the highest number of purchases")

q1=final_df.groupBy("Customer Name").agg(max("Quantity").alias("Max_Quantity")).orderBy(desc("Max_Quantity")).limit(1)
q1_pandas=q1.toPandas()
st.dataframe(q1_pandas)

#Q2.What is the average discount given on sales across all products
st.write ("Q2.What is the average discount given on sales across all products")

q2=final_df.groupBy("Sub-Category").agg(avg("Discount").alias("Avg_Discount"))
q2_pandas=q2.toPandas()
st.dataframe(q2_pandas)

#Q3.How many unique products were sold in each region

st.write ("Q3.How many unique products were sold in each region")

q3=final_df.groupBy("Region").agg(countDistinct("Sub-Category").alias("Count_Product"))
q3_pandas=q3.toPandas()
st.dataframe(q3_pandas)

#Q4.What is the total profit generated in each state

st.write ("Q4.What is the total profit generated in each state")

q4=final_df.groupBy("State").agg(sum("Profit").alias("Total_Profit"))
q4_pandas=q4.toPandas()
st.dataframe(q4_pandas)


#Q5.Which product sub-category has the highest sales

st.write ("Q5.Which product sub-category has the highest sales")

q5=final_df.groupBy("Sub-Category").agg(max("Sales").alias("Highest_Sales")).orderBy(desc("Highest_Sales")).limit(1)
q5_pandas=q5.toPandas()
st.dataframe(q5_pandas)

#Q6.What is the average age of customers in each segment

st.write ("Q6.What is the average age of customers in each segment")

q6=final_df.groupBy("Segment").agg(avg("Age").alias("Average_age"))
q6_pandas=q6.toPandas()
st.dataframe(q6_pandas)

#Q7.How many orders were shipped in each shipping mode.

st.write ("Q7.How many orders were shipped in each shipping mode.")

q7=final_df.groupBy("Ship Mode").agg(count("Order ID").alias("Count_order"))
q7_pandas=q7.toPandas()
st.dataframe(q7_pandas)

#Q8.What is the total quantity of products sold in each city
st.write ("Q8.What is the total quantity of products sold in each city")

q8=final_df.groupBy("City").agg(count("Order ID").alias("order_count"))
q8_pandas=q8.toPandas()
st.dataframe(q8_pandas)

#Q9.Which customer segment has the highest profit margin

st.write ("Q9.Which customer segment has the highest profit margin")

q9=final_df.withColumn("Profit_Margin",col("Profit")/col("Sales"))\
.groupBy("Segment").agg(max("Profit_Margin").alias("Highest_Profit_Margin")).orderBy(desc("Highest_Profit_Margin")).limit(1)
q9_pandas=q9.toPandas()
st.dataframe(q9_pandas)

