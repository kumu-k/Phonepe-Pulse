# Phonepe-Pulse

# Phonepe Pulse Data Visualization and Exploration:
A User-Friendly Tool Using  Plotly

1.This project extract data from the Phonepe pulse Github repository through scripting and
clone it.
2. Transform the data into a suitable format and perform any necessary cleaningand pre-processing steps.
3. Insert the transformed data into a MySQL database for efficient storage and retrieval.
4. Create a live geo visualization dashboard using Plotly in Python to display the data in an interactive and visually appealing manner.
5. Fetch the data from the MySQL database to display in the dashboard.
6. Provide at least 1 different dropdown options for users to select different facts and figures to display on the dashboard.



## API Reference

#### Table years
create_table_query = '''CREATE TABLE year(
                                success BOOLEAN,
                                code VARCHAR(255),
                                responseTimestamp DATETIME,
                                data_from DATETIME,
                                data_to DATETIME,
                                names VARCHAR(255),
                                totcounts INT,
                                totamounts FLOAT
                                );

##Library
!pip install jupyter-dash
!pip install --upgrade jupyter-dash
!pip install plotly
!pip install mysql-connector-python
!pip install pymysql
!pip install mysql-connector



## Installation

run the code in jupter notebook
press the plotly GUI link to view the output

##Future implement

1.Include 9 dropdownlist .
2.Each dropdownlist need support with different visualization charts.
    
