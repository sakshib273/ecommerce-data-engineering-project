
# End-To-End Ecommerce Data Engineering Project

This project showcases the development of a comprehensive data engineering pipeline for an eCommerce platform. The pipeline is designed to handle data ingestion, transformation, and loading (ETL), utilizing various AWS services and Python libraries.


### Technologies used:  
AWS S3, Athena, Glue, Redshift, pandas, Jupyter Notebooks.
### Project Workflow:

- Data Storage: Raw eCommerce data is stored in Amazon S3.
- Metadata Extraction: AWS Glue Crawler retrieves metadata from S3.
- Data Loading: Data is queried from S3 into Jupyter Notebooks using Amazon Athena.
- Data Transformation: Data is cleaned and transformed with pandas.
- Output Storage: Transformed data is saved back to S3.
- Data Warehousing: AWS Glue loads data from S3 into Amazon Redshift.
- Visualization: Insights into customer behavior and product performance are visualized.

### Dataset link :
    https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/
## Architecture Map

![Architecture](https://github.com/user-attachments/assets/98ce0562-f1b8-4a0d-b817-84170daa539c)

## Data Model

![Ecommerce_data_model](https://github.com/user-attachments/assets/545210c2-4410-4988-82e4-b10e51605da6)


## Dimensional mode

![ecomm_dimension_model](https://github.com/user-attachments/assets/c12da968-2d6c-472b-8df3-6df2069d4a27)

## Snapshot of the Dashboard

![Screenshot (116)](https://github.com/user-attachments/assets/1e059eea-a228-4497-9640-dd9985cd4452)







