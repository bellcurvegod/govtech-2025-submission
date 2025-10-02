from datetime import datetime, date
import pandas as pd
import sqlite3

# Extract - Load orders and product CSVs into DF
orders_df = pd.read_csv("orders.csv")
products_df = pd.read_csv("products.csv")

sales_db = "sales.db"

# Transform 
# Revenue for each order line item
orders_df['Revenue'] = orders_df['Quantity'] * orders_df['Price']

# Calculate OrderYear, OrderMonth, and OrderDay
orders_df["OrderDate"] = pd.to_datetime(orders_df["OrderDate"])
orders_df['OrderYear'] = orders_df['OrderDate'].dt.year
orders_df['OrderMonth'] = orders_df['OrderDate'].dt.month
orders_df['OrderDay'] = orders_df['OrderDate'].dt.day

# Left join on ProductID
merged_df = pd.merge(orders_df,products_df, on="ProductID", how="left")

# Load
conn = sqlite3.connect(sales_db)
# Create dimension table for products


# Create dimension table for dates 




