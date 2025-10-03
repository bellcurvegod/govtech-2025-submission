
"""
Govtech 2025 Take Home Assessment: Sales ETL Pipeline

This module extracts sales data from CSV files orders.csv and products.csv, transforms it according to business requirements,
and loads it into a star schema data warehouse for analytical reporting.

By: Rachel Goh
"""

from datetime import datetime, date
import pandas as pd
import sqlite3
import sys

# Extract data from CSV files into Pandas dataframes
def extract(order_file="orders.csv", product_file="products.csv"):
    try:
        orders_df = pd.read_csv(order_file)
        products_df = pd.read_csv(product_file)
        return orders_df, products_df
    except Exception as e:
        sys.exit(f"Extraction error: {e}")

# Transform raw data according to business requirements
# Calculates revenue, splits dates into individual parts, and does a left join with product details
def transform(orders_df, products_df):
    try:
        # Calculate revenue
        orders_df['Revenue'] = orders_df['Quantity'] * orders_df['Price']

        # Convert OrderDate to datetime and get Year/Month/Day
        orders_df["OrderDate"] = pd.to_datetime(orders_df["OrderDate"])
        orders_df['OrderYear'] = orders_df['OrderDate'].dt.year
        orders_df['OrderMonth'] = orders_df['OrderDate'].dt.month
        orders_df['OrderDay'] = orders_df['OrderDate'].dt.day

        # Merge with product data
        merged_df = pd.merge(orders_df, products_df, on="ProductID", how="left")
        return merged_df
    except Exception as e:
        sys.exit(f"Error in transformation: {e}")

# Load transformed data into SQLite database using star schema
def load(merged_df, products_df, db_name="sales.db"):
    try:
        conn = sqlite3.connect(db_name)
        cur = conn.cursor()

        # Create product dimension table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_products (
            ProductID TEXT PRIMARY KEY,
            ProductName TEXT,
            Category TEXT,
            Cost REAL
        )""")

        # Create date dimension table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_date (
            DateID INTEGER PRIMARY KEY AUTOINCREMENT,
            Date TEXT,
            Year INTEGER,
            Month INTEGER,
            Day INTEGER
        )""")

        # Create customer dimension table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_customers (
            CustomerID TEXT PRIMARY KEY
        )""")

        # Create fact table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_sales (
            OrderID INTEGER PRIMARY KEY,
            ProductID TEXT,
            CustomerID TEXT,
            DateID INTEGER,
            Quantity INTEGER,
            Price REAL,
            Revenue REAL,
            FOREIGN KEY (ProductID) REFERENCES dim_products(ProductID),
            FOREIGN KEY (CustomerID) REFERENCES dim_customers(CustomerID),
            FOREIGN KEY (DateID) REFERENCES dim_date(DateID)
        )""")

        # load product dimension
        products_df.drop_duplicates().to_sql("dim_products", conn, if_exists="replace", index=False)

        # load date dimension
        date_dim = merged_df[['OrderDate', 'OrderYear', 'OrderMonth', 'OrderDay']].drop_duplicates()
        date_dim = date_dim.rename(columns={
            'OrderDate': 'Date',
            'OrderYear': 'Year',
            'OrderMonth': 'Month',
            'OrderDay': 'Day'
        })
        date_dim.to_sql("dim_date", conn, if_exists="replace", index=False)

        # load customer dimension
        customer_dim = merged_df[['CustomerID']].drop_duplicates()
        customer_dim.to_sql("dim_customers", conn, if_exists="replace", index=False)

        # load fact table
        fact_df = merged_df[[
            'OrderID', 'ProductID', 'CustomerID', 'Quantity', 'Price', 'Revenue', 'OrderDate'
        ]].copy()

        # map OrderDate to DateID
        date_lookup = pd.read_sql("SELECT rowid as DateID, Date FROM dim_date", conn)
        fact_df = fact_df.merge(date_lookup, left_on="OrderDate", right_on="Date")
        fact_df = fact_df.drop(columns=["OrderDate", "Date"])
        fact_df.to_sql("fact_sales", conn, if_exists="replace", index=False)

        conn.commit()
    except Exception as e:
        sys.exit(f"Error in load: {e}")
    finally:
        conn.close()


def main():
    orders_df, products_df = extract()
    merged_df = transform(orders_df, products_df)
    load(merged_df, products_df)
    print("ETL pipeline completed")


if __name__ == "__main__":
    main()