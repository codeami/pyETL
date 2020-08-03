# pyETL - pyTests for ETL pipelines/ Data Warehouses

Data Content Validation solution with GUI to configure and run python tests (pyTest)

![GUI Runner](https://https://github.com/codeami/pyETL/GUI.png)

Main script- pandas_helper.py 

## PyETL Components
- pandas (python data processing library)
- yaml (txt based configuration)
- python transforms (string functions, data types operations)
- xlsx (difference report with formatting to highlight difference)
- results Db (send results to database)
- python Db drivers added: Oracle, SQL Server, Posgres, Snowflake, new ones can be easily added.
- Test on Windows and Linux both

## Framework Features - GUI / Yaml config
* Table-wise Content validation
  - Choose Source type,  
    - and 1 Hop – Source and Target Dbs 
    - and 2 Hops – #Todo
      - Source to Target1 and Source to Target2 (without calling Source twice)
      - Source to Target1 and Target1 to Target2 (without calling Target1 twice)
 - Filter by schemas- Choose 1 or more schemas
    - All tables under schema will be run
 - Filter by tables - Choose 1 or more tables
    - Only select tables will be run
 - Filter by tags – 
    - Choosing tags. Ex: exclude tables with tag=slow *Upcoming
* Column-wise Content validation 
    - Save and reuse Query pairs for custom Select Col1,Col2 .. queries

## Extract Features
* Select * from Schema.Table –ETL test
  - Size check before extraction - avoid single test hogging all resources
    - Abort extraction if above threshold  ~2-3 GB table size 
        - Virtual memory utilization
    - Friendly warning recommending partitioning select * query
      - Automated partitioning logic ^upcoming feature
  - Fast native Db connectors
    - Ex: CAPS Oracle X MB/sec, LIGHTHOUSE MSSQL, Postgres X MB/sec
    - Time taken to extract + Compare logged in Database
* Select Col1, Col2 from Schema.Table
  -EDW Testing
  -ETL Subset Testing ^upcoming feature
* Load previous results from Cache
  -helps add custom transforms – essential Business rules
  -Local disk utilization 
* Extract rowcounts using single query for all tables in a schema
  -Improved efficiency

## Custom Transform Features
* Columns check after dropping ETL/DG columns
  -Fail test on columns mismatch - highlighting missing columns 
  -1:1 column name mapping validated in select * query
  -Column names from Source and Target concatenated in select col1,.. query
    -Concatenated in order  - so columns in query must be ordered correctly
      -See query in GUI interface
    -‘|’ pipe separated column names
* Automated casting (type conversion)
  -Logic is coded to smartly cast Data types
  -Ex: Columns names with date/time in them are cast to DateTime type
  
 ## Test Details in a YAML (dictionary format):-
* Connector1 = Source connector
* Connector2 = Target connector
* Query1 = Source query
* Query2 = Target query
* P_key = primary key
* Use_cache = True (enable caching feature)
    - Transforms = User defined transforms
      - Table1
        - Column1
            - Transform1
            - Transform2
            - ..
        - Column2
            - Transform1
            - ..
      - Table2


  
