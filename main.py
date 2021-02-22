import requests
import pypyodbc as odbc # pip install pypyodbc
import pandas as pd # pip install pandas

csvURL = "https://data.austintexas.gov/api/views/dx9v-zd7x/rows.csv?accessType=DOWNLOAD"

# URL of the image to be downloaded is defined as image_url 
r = requests.get(csvURL) # create HTTP response object 
# send a HTTP request to the server and save 
# the HTTP response in a response object called r 
with open("Incidents.csv",'wb') as f: 
    # Saving received content as a png file in 
    # binary format  
    # write the contents of the response (r.content) 
    # to a new file in binary mode. 
    f.write(r.content) 

# Step 1. Importing dataset from CV file (might need to download it first)
df = pd.read_csv('Incidents.csv')
# Step 2. Data clean up -- converting the 2 dates in the fomrat that we prefer
df['Published Date'] = pd.to_datetime(df['Published Date']).dt.strftime('%Y-%m-%d %H:%M:%S')
df['Status Date'] = pd.to_datetime(df['Status Date']).dt.strftime('%Y-%m-%d %H:%M:%S')
# Drop the empty rows for fields: location,status
df.drop(df.query('Location.isnull() | Status.isnull()').index, inplace=True)

# Step 3. Specify the columns we would like to import
# Traffic Report ID,Published Date,Issue Reported,Location,Latitude,Longitude,Address,Status,Status Date
columns = ['Traffic Report ID', 'Published Date', 'Issue Reported', 'Location', 'Address', 'Status', 'Status Date']

df_data = df[columns]
records = df_data.values.tolist()

# Step 4.1 Create SQL Server Connection String
#  
DRIVER = 'SQL Server'
SERVER_NAME = 'bncsql'
DATABASE_NAME = 'bncSqlCV'

def connection_string(driver, server_name, database_name):
    conn_string = f"""
        DRIVER={{{driver}}};
        SERVER={server_name};
        DATABASE={database_name};
        Trust_Connection=yes;        
    """
    return conn_string

"""
Step 4.2 Create database connection instance
"""
try:
    conn = odbc.connect(connection_string(DRIVER, SERVER_NAME, DATABASE_NAME))
except odbc.DatabaseError as e:
    print('Database Error:')    
    print(str(e.value[1]))
except odbc.Error as e:
    print('Connection Error:')
    print(str(e.value[1]))


"""
Step 4.3 Create a cursor connection and insert records
"""

sql_insert = '''
    INSERT INTO Austin_Traffic_Incident 
    VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE())
'''

try:
    cursor = conn.cursor()
    cursor.executemany(sql_insert, records)
    cursor.commit();    
except Exception as e:
    cursor.rollback()
    print(str(e[1]))
finally:
    print('Task is complete.')
    cursor.close()
    conn.close()