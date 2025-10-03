from datetime import datetime, date
import pandas as pd
import sqlite3

# Extract - Load orders and product CSVs into DF
orders_df = pd.read_csv("orders.csv")
products_df = pd.read_csv("products.csv")

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
sales_db = "sales.db"
conn = sqlite3.connect(sales_db)
cur = conn.cursor()

# Create dimension table for products
cur.execute(""""
CREATE TABLE IF NOT EXISTS dim_products (
    ProductID TEXT PRIMARY KEY
    ProductName TEXT,
    Category TEXT,
    Cost REAL         
)
""")

# Create dimension table for order dates
cur.execute("""
CREATE TABLE IF NOT EXISTS dim_date (
    DateID Integer PRIMARY KEY,
    Date TEXT, 
    Year INTEGER, 
    Month INTEGER,
    Day INTEGER
)
""")

# Create dimension table for customers
cur.execute("""
CREATE TABLE IF NOT EXISTS dim_customers (
    CustomerID TEXT PRIMARY KEY            
)
""")

