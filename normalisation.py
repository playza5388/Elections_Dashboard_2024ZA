import pyodbc
import os
import csv
import pandas as pd

# Babalwe Doda
# creating the Province table

def Insert_into_Province_table():

    current_directory = os.path.dirname(os.path.abspath(__file__))

    # Combine the current directory with the CSV file name
    file_path = os.path.join(current_directory, 'National.csv')
    provinces = set()

    # Open the CSV file
    # Read the CSV file using pandas
    df = pd.read_csv(file_path, usecols=["Province"], dtype=str)

    # Filter out any rows where the Province column is NaN
    df = df.dropna(subset=["Province"])

    # Get unique province names
    unique_provinces = df["Province"].unique()
    
    #print(unique_provinces)

   
    db_file_path = os.path.join(current_directory, 'National.accdb')

    # Construct the connection string
    conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_file_path};'

    # Connect to the database
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    for Province in unique_provinces:
        cursor.execute("INSERT INTO Province (ProvinceName) VALUES (?)", (Province,))
        print(Province)
      
    conn.commit()
    cursor.close()

def Insert_into_PoliticalParty_table():

    current_directory = os.path.dirname(os.path.abspath(__file__))

    # Combine the current directory with the CSV file name
    file_path = os.path.join(current_directory, 'National.csv')
    provinces = set()

    # Open the CSV file
    # Read the CSV file using pandas
    df = pd.read_csv(file_path, usecols=["sPartyName"], dtype=str)

    # Filter out any rows where the Province column is NaN
    df = df.dropna(subset=["sPartyName"])

    # Get unique province names
    parties = df["sPartyName"].unique()
    

   
    db_file_path = os.path.join(current_directory, 'National.accdb')

    # Construct the connection string
    conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_file_path};'

    # Connect to the database
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    for Party in parties:
        #cursor.execute("INSERT INTO PParty (PartyName) VALUES (?)", (Party,))
        print(Party)
      
    #conn.commit()
    cursor.close()

def Inserting_into_Municipality_table():

    current_directory = os.path.dirname(os.path.abspath(__file__))

    # Combine the current directory with the CSV file name
    file_path = os.path.join(current_directory, 'National.csv')
    provinces = set()

    df = pd.read_csv(file_path)
    unique_municipalities = df[['Province', 'Municipality']].drop_duplicates()
    
    db_file_path = os.path.join(current_directory, 'National.accdb')

    # Construct the connection string
    conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_file_path};'

    # Connect to the database
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Identifying foreign key in the Province table then inserting into Municipalities table

    for index, row in unique_municipalities.iterrows():
            cursor.execute("SELECT ProvinceID FROM Province WHERE ProvinceName = ?", (row["Province"],))
            data = (row["Municipality"],cursor.fetchall()[0][0])
            cursor.execute("INSERT INTO Municipality (MunicipalityName, ProvinceID) VALUES (?, ?)", data)
    
    conn.commit()
    cursor.close()

def Inserting_into_VD_table():
     
    current_directory = os.path.dirname(os.path.abspath(__file__))

    file_path = os.path.join(current_directory, 'National.csv')
    df = pd.read_csv(file_path)
    unique_VD = df[['Municipality','VD_Number','VS_Name','Registered_Population']].drop_duplicates()


    db_file_path = os.path.join(current_directory, 'National.accdb')

    # Construct the connection string
    conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_file_path};'

    # Connect to the database
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    for index, row in unique_VD.iterrows():
            cursor.execute("SELECT MunicipalityID FROM Municipality WHERE MunicipalityName = ?", (row["Municipality"],))
            data = (cursor.fetchall()[0][0],row["VD_Number"],row["VS_Name"],row["Registered_Population"])
            cursor.execute("INSERT INTO VDistrict (MunicipalityID,VD_Number,VS_Name,Registered_Population ) VALUES (?, ?, ?, ?)", data)
    
    conn.commit()
    cursor.close()

def Insert_into_EResult_table():
     
    current_directory = os.path.dirname(os.path.abspath(__file__))

    file_path = os.path.join(current_directory, 'National.csv')
    df = pd.read_csv(file_path)
    unique_ER = df[['VD_Number','Spoilt_Votes','Total_Valid_Votes','Generated_Datetime']].drop_duplicates()

    db_file_path = os.path.join(current_directory, 'National.accdb')

    # Construct the connection string
    conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_file_path};'

    # Connect to the database
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    for index, row in unique_ER.iterrows():
            cursor.execute("SELECT VD_ID FROM VDistrict WHERE VD_Number = ?", (row["VD_Number"],))
            data = (cursor.fetchall()[0][0],row["Spoilt_Votes"],row["Total_Valid_Votes"],row["Generated_Datetime"])
            cursor.execute("INSERT INTO EResult (VD_ID,SpoiltVotes,TotalValidVotes,Generated_Date ) VALUES (?, ?, ?, ?)", data)

    conn.commit()
    cursor.close()

def Insert_into_PVotes_table():
    current_directory = os.path.dirname(os.path.abspath(__file__))

    file_path = os.path.join(current_directory, 'National.csv')
    df = pd.read_csv(file_path)
    unique_partyVotes = df[['Province','Municipality','VD_Number','VS_Name','Registered_Population','Spoilt_Votes','Total_Valid_Votes','sPartyName','Party_Votes']].drop_duplicates()

    db_file_path = os.path.join(current_directory, 'National.accdb')

    # Construct the connection string
    conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_file_path};'

    # Connect to the database
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    for index, row in unique_partyVotes.iterrows():
        cursor.execute("SELECT PartyID FROM PParty WHERE PartyName = ?", (row["sPartyName"],))
        partyID = cursor.fetchall()[0][0]

        query = """
                SELECT 
                er.[ResultID]
                FROM ((EResult AS er INNER JOIN VDistrict AS vd ON er.[VD_ID] = vd.[VD_ID]) INNER JOIN Municipality AS m ON vd.[MunicipalityID] = m.[MunicipalityID]) INNER JOIN Province AS p ON m.ProvinceID = p.ProvinceID
                WHERE 
                p.[ProvinceName] = ?
                AND m.[MunicipalityName] = ?
                AND vd.[VD_Number] = ?
                AND vd.[VS_Name] = ?
                AND vd.[Registered_Population] = ?
                AND er.[SpoiltVotes] = ?
                AND er.[TotalValidVotes] = ?;
                """
        values = (
            row['Province'], 
            row['Municipality'], 
            row['VD_Number'], 
            row['VS_Name'], 
            row['Registered_Population'], 
            row['Spoilt_Votes'], 
            row['Total_Valid_Votes']
        )
        cursor.execute(query, values)
        ResultID = cursor.fetchall()[0][0]
        #Now you can insert into PVotes with the obtained ResultID
        cursor.execute("INSERT INTO PVotes (ResultID, PartyID, Votes) VALUES (?, ?, ?)", (ResultID, partyID, row['Party_Votes']))
    
    conn.commit()
    cursor.close()
    conn.close()



     

def main():

    Insert_into_PoliticalParty_table()
    print("Done")
    

main()