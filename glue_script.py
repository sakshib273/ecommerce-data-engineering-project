# Glue Script 

import redshift_connector
import pandas as pd
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

host = config.get('redshift','HOST')
port = config.get('redshift','PORT')
database = config.get('redshift','DATABASE')
user = config.get('redshift','USER')
password = config.get('redshift','PASSWORD')
iam_role = config.get('s3','IAM_ROLE')

try:
    conn = redshift_connector.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    conn.autocommit = True
    cursor = conn.cursor()

    # Create tables
    cursor.execute("""
    CREATE TABLE "dim_geolocation" (
        "index" INTEGER,
        "geolocation_zip_code_prefix" INTEGER,
        "geolocation_lat" REAL,
        "geolocation_lng" REAL,
        "geolocation_city" TEXT,
        "geolocation_state" TEXT
    )
    """)
    
    cursor.execute("""
    CREATE TABLE "dim_customers" (
        "index" INTEGER,
        "customer_id" TEXT,
        "customer_unique_id" TEXT,
        "customer_zip_code_prefix" INTEGER,
        "customer_city" TEXT,
        "customer_state" TEXT
    )
    """)
    
    cursor.execute("""
    CREATE TABLE "dim_sellers" (
        "index" INTEGER,
        "seller_id" TEXT,
        "seller_zip_code_prefix" INTEGER,
        "seller_city" TEXT,
        "seller_state" TEXT
    )
    """)
    
    cursor.execute("""
    CREATE TABLE "fact_orders_info" (
        "index" INTEGER,
        "order_id" TEXT,
        "order_item_id" INTEGER,
        "product_id" TEXT,
        "seller_id" TEXT,
        "shipping_limit_date" TEXT,
        "price" REAL,
        "freight_value" REAL,
        "customer_id" TEXT,
        "payment_sequential" REAL,
        "payment_type" TEXT,
        "payment_installments" REAL,
        "payment_value" REAL,
        "review_id" TEXT,
        "review_score" TEXT
    )
    """)
    
    cursor.execute("""
    CREATE TABLE "dim_products" (
        "index" INTEGER,
        "product_id" TEXT,
        "product_category_name" TEXT,
        "product_category_name_english" TEXT,
        "product_name_length" REAL,
        "product_description_length" REAL,
        "product_photos_qty" REAL,
        "product_weight_g" REAL,
        "product_length_cm" REAL,
        "product_height_cm" REAL,
        "product_width_cm" REAL
    )
    """)
    
    cursor.execute("""
    CREATE TABLE "orders_date_dim" (
        "index" INTEGER,
        "order_id" TEXT,
        "customer_id" TEXT,
        "order_status" TEXT,
        "order_purchase_timestamp_key" INTEGER,
        "order_approved_at_key" INTEGER,
        "order_delivered_carrier_date_key" INTEGER,
        "order_delivered_customer_date_key" INTEGER,
        "order_estimated_delivery_date_key" INTEGER
    )
    """)
    
    cursor.execute("""
    CREATE TABLE "date_dim" (
        "index" INTEGER,
        "date" TIMESTAMP,
        "date_id" INTEGER,
        "day" INTEGER,
        "month" INTEGER,
        "year" INTEGER,
        "quarter" INTEGER,
        "week" INTEGER,
        "day_of_week" TEXT
    )
    """)
    
    cursor.execute("""
    CREATE TABLE "dim_reviews" (
        "index" INTEGER,
        "review_id" TEXT,
        "order_id" TEXT,
        "review_comment_title" TEXT,
        "review_comment_message" TEXT,
        "review_creation_date" TIMESTAMP,
        "review_answer_timestamp" TIMESTAMP
    )
    """)

    # Load data from S3 into tables
    cursor.execute("""
    COPY dim_geolocation
    FROM 's3://sakshi-ecommerce-de-project/files/output/dim_geolocation.csv'
    IAM_ROLE '{iam_role}'
    DELIMITER ','
    IGNOREHEADER 1
    REGION 'ap-south-1';
    """)

    cursor.execute("""
    COPY dim_customers
    FROM 's3://sakshi-ecommerce-de-project/files/output/dim_customers.csv'
    IAM_ROLE '{iam_role}'
    DELIMITER ','
    IGNOREHEADER 1
    REGION 'ap-south-1';
    """)

    cursor.execute("""
    COPY dim_sellers
    FROM 's3://sakshi-ecommerce-de-project/files/output/dim_sellers.csv'
    IAM_ROLE '{iam_role}'
    DELIMITER ','
    IGNOREHEADER 1
    REGION 'ap-south-1';
    """)

    cursor.execute("""
    COPY fact_orders_info
    FROM 's3://sakshi-ecommerce-de-project/files/output/fact_orders_info.csv'
    IAM_ROLE '{iam_role}'
    DELIMITER ','
    IGNOREHEADER 1
    REGION 'ap-south-1';
    """)

    cursor.execute("""
    COPY dim_products
    FROM 's3://sakshi-ecommerce-de-project/files/output/dim_products.csv'
    IAM_ROLE '{iam_role}'
    DELIMITER ','
    IGNOREHEADER 1
    REGION 'ap-south-1';
    """)

    cursor.execute("""
    COPY orders_date_dim
    FROM 's3://sakshi-ecommerce-de-project/files/output/orders_date_dim.csv'
    IAM_ROLE '{iam_role}'
    DELIMITER ','
    IGNOREHEADER 1
    REGION 'ap-south-1';
    """)

    cursor.execute("""
    COPY date_dim
    FROM 's3://sakshi-ecommerce-de-project/files/output/date_dim.csv'
    IAM_ROLE '{iam_role}'
    DELIMITER ','
    IGNOREHEADER 1
    REGION 'ap-south-1';
    """)

    cursor.execute("""
    COPY dim_reviews
    FROM 's3://sakshi-ecommerce-de-project/files/output/dim_reviews.csv'
    IAM_ROLE '{iam_role}'
    DELIMITER ','
    IGNOREHEADER 1
    REGION 'ap-south-1';
    """)

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    cursor.close()
    conn.close()
