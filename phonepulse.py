#!/usr/bin/env python
# coding: utf-8

# In[11]:


get_ipython().system('pip install jupyter-dash')
get_ipython().system('pip install --upgrade jupyter-dash')


# In[12]:


get_ipython().system('pip install plotly')


# **Downloading Data from github**

# In[26]:


import pandas as pd
import urllib.request
import json

# list of years and quarters
years = [2018, 2019, 2020, 2021, 2022]
quarters = [1, 2, 3, 4]

# create an empty list to store dataframes
dfs = []

# iterate over years and quarters
for year in years:
    for quarter in quarters:
        # create the file path
        file_path = f"https://raw.githubusercontent.com/PhonePe/pulse/master/data/aggregated/transaction/country/india/{year}/{quarter}.json"

        try:
            # read the json file
            with urllib.request.urlopen(file_path) as url:
                data = json.loads(url.read().decode())
            # convert data to a dataframe
            df = pd.json_normalize(data)
            # add the dataframe to the list
            dfs.append(df)
        except urllib.error.HTTPError as e:
            print(f"Error downloading file for year {year}, quarter {quarter}: {e}")

# concatenate all dataframes into a single dataframe
result = pd.concat(dfs)
js=pd.concat(dfs)
result


# In[27]:


result.head()


# In[28]:


print(result.columns)


# In[29]:


result.shape


# **Data Cleaning**

# In[30]:


# Print info of DataFrame
result.info


# In[32]:


# Print data types of DataFrame
result.dtypes


# In[33]:


del result['data.transactionData']
result.head()


# In[34]:


for col in result.columns[0:]:
    print(col, ': ', len(result[col].unique()), 'labels')


# In[35]:


# Print number of missing values
result.isnull().sum()


# In[36]:


# Print description of DataFrame
result.describe()


# ##### **responseTimestamp** Convert `responseTimestamp` column to `datetime`

# In[37]:


# Print header of column
result['responseTimestamp'].head()


# In[38]:


# Convert column to datetime
result['responseTimestamp'] = pd.to_datetime(js['responseTimestamp'], format = '%Y-%m-%d')
result['data.from'] = pd.to_datetime(js['data.from'], format = '%Y-%m-%d')
result['data.to'] = pd.to_datetime(js['data.to'], format = '%Y-%m-%d')


# In[39]:


# Print header and datatypes of both columns again
print(result['responseTimestamp'].head())
print(result['responseTimestamp'].dtypes)

print(result['data.from'].head())
print(result['data.from'].dtypes)

print(result['data.to'].head())
print(result['data.to'].dtypes)


# ##### **success** Check field a make it only contain boolean value TRUE/False

# In[40]:


# Find number of unique values in success column
result['success'].unique()


# In[41]:


# How many values of different success do we have?
result['success'].value_counts()


# In[42]:


# check if each column contains only True and False
#print(df['col1'].unique().tolist() == [True, False])  # True
print(result['success'].isin([True, False, 'true', 'false', 'TRUE', 'FALSE']).all())


# ##### **Code** Check field a make it only contain valid value removing trailing spaces and change racter to uppercase

# In[43]:


# Find number of unique values in code column
result['code'].unique()


# In[44]:


# How many values of different code do we have?
result['code'].value_counts()


# In[45]:


#remove trailing whitespace
result['code'] = result['code'].str.strip()
result.head()


# In[46]:


#convert all characters to uppercase
result['code'] = result['code'].str.upper()
result.head()


# 

# In[47]:


# create a new data frame with selected columns
df1 = js.loc[:, ['data.transactionData']]

# print the new data frame
df1.head(2)


# In[49]:


import pandas as pd

def clean_data(row):
    counts = []
    amounts = []
    names = []
    # iterate over the payment categories
    for payment in row:
        names.append(payment['name'])
        counts.append(payment['paymentInstruments'][0]['count'])
        amounts.append(payment['paymentInstruments'][0]['amount'])
    # create new columns for the counts and amounts
    return pd.Series({'Names': names, 'Counts': counts, 'Amounts': amounts})

# apply the function to the dataframe column
df1[['Names', 'Counts', 'Amounts']] = df1['data.transactionData'].apply(clean_data)


# In[50]:


# drop the original metric column
df1.drop('data.transactionData', axis=1, inplace=True)


# In[51]:


df1['Names'] = df1['Names'].apply(lambda x: ', '.join(x))
df1['Counts'] = df1['Counts'].apply(lambda x: ', '.join(str(i) for i in x))
df1['Amounts'] = df1['Amounts'].apply(lambda x: ', '.join(str(i) for i in x))


# In[52]:


df1.head()


# In[53]:


df1.shape


# In[ ]:


# create empty lists for names, total counts, and total amounts
names = []
total_counts = []
total_amounts = []

# iterate over the rows in df1
for index, row in df1.iterrows():
    names.append(row['Names'])
    counts_sum = sum(row['Counts'])
    total_counts.append(counts_sum)
    amounts_sum = sum(row['Amounts'])
    total_amounts.append(amounts_sum)

# create a new DataFrame from the lists
df2 = pd.DataFrame({'Names': names, 'Total Counts': total_counts, 'Total Amounts': total_amounts})


# In[62]:


df2.shape


# In[81]:


ndf = pd.concat([result, df1], axis=1)



# In[82]:


ndf.shape
#ndf.columns


# In[92]:


ndf


# In[93]:


ndf.head()


# In[72]:


result.reset_index(drop=True, inplace=True)
df2.reset_index(drop=True, inplace=True)
tdf = pd.concat([result, df2], axis=1)


# In[75]:


tdf.shape
#tdf.columns


# In[85]:


tdf.head()


# In[86]:


# Print number of missing values
ndf.isnull().sum()


# In[87]:


# Print number of missing values
tdf.isnull().sum()


# In[89]:


get_ipython().system('pip install pymysql')


# In[90]:


import mysql.connector as sql
mydb = sql.connect(
  host="localhost",
  user="root",
  password="",  
)

print(mydb)
mycursor = mydb.cursor(buffered=True)


# In[91]:


ndf.dtypes


# In[92]:


tdf.dtypes


# In[94]:


mycursor.execute("CREATE DATABASE phonepepulse")


# In[7]:


mycursor.execute("USE phonepepulse")


# In[122]:


# create table
create_table_query = '''CREATE TABLE year(
                                success BOOLEAN,
                                code VARCHAR(255),
                                responseTimestamp DATETIME,
                                data_from DATETIME,
                                data_to DATETIME,
                                names VARCHAR(255),
                                totcounts INT,
                                totamounts FLOAT
                                ); '''
 # execute create table query
mycursor.execute(create_table_query)
print("Table created successfully ")


# In[130]:


import datetime

for i, row in tdf.iterrows():
    # Convert the list of names to a comma-separated string
    names_list = row['Names']
    names_str = ', '.join(names_list)
    
    # Define the insert query and values
    insert_query = "INSERT INTO year (success, code, responseTimestamp, data_from, data_to, names, totcounts, totamounts) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    values = [
        row['success'], 
        row['code'], 
        row['responseTimestamp'].strftime('%Y-%m-%d %H:%M:%S'), 
        row['data.from'].strftime('%Y-%m-%d %H:%M:%S'), 
        row['data.to'].strftime('%Y-%m-%d %H:%M:%S'), 
        names_str, # Use the string of names here
        row['Total Counts'], 
        row['Total Amounts']
    ]
    
    # Execute the insert query
    mycursor.execute(insert_query, values)
    print("Dataframe row inserted successfully")

# After all the rows have been inserted, commit the changes to the database
mydb.commit()

# Close the cursor and database connection
mycursor.close()
mydb.close()


# In[4]:


mycursor.execute("USE phonepepulse")


# In[ ]:


#ondedropdownlist update by pie char 
import plotly.graph_objs as go
import plotly.offline as pyo
import mysql.connector
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

# connect to the database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="phonepepulse"
)

# retrieve the data from the database
mycursor = mydb.cursor()
mycursor.execute("SELECT Names, totamounts, totcounts FROM year")

# extract the data into lists
names = []
amounts = []
counts = []

for row in mycursor:
    names.append(row[0])
    amounts.append(row[1])
    counts.append(row[2])

# create the dropdown options
dropdown_options = [
    {'label': 'Total Amounts', 'value': 'amounts'},
    {'label': 'Total Counts', 'value': 'counts'}
]

# create the layout
layout = go.Layout(title='PhonePe Pulse')

# create the chart figure
fig = go.Figure(layout=layout)

# add the initial trace
fig.add_trace(go.Pie(labels=names, values=amounts, name='Total Amounts'))
fig.update_layout(title='Total Amounts by Names', showlegend=True)

# create the app
app = dash.Dash(__name__)

# create the app layout
app.layout = html.Div([
    html.H1("PhonePe Pulse"),
    dcc.Dropdown(id='my_dropdown',
                 options=dropdown_options,
                 value='amounts'),
    dcc.Graph(id='my_graph', figure=fig)
])

# create the callback function
@app.callback(Output('my_graph', 'figure'),
              Input('my_dropdown', 'value'))
def update_graph(selected_value):
    # determine which data to use based on the selected dropdown value
    data = amounts if selected_value == 'amounts' else counts
    
    # update the chart trace
    fig.update_traces(values=data)

    # update the chart title
    title = 'Total Amounts by Names' if selected_value == 'amounts' else 'Total Counts by Names'
    fig.update_layout(title=title)

    # return the updated figure
    return fig

# run the app
if __name__ == '__main__':
    app.run_server()


# In[ ]:





# In[ ]:





# In[ ]:




