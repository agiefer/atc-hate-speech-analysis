import pandas as pd
import json
from collections import namedtuple
import sqlite3
import sys
import config



def connect2Database():
    connection = sqlite3.connect(config.databaseName, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    return connection

def dropDatabase():
    
    connection = connect2Database()
    cursor = connection.cursor()

    query = f"DROP TABLE IF EXISTS {config.analysisTableName}"
    cursor.execute(query)

# creates database with all tables
def createDatabase():

    connection = connect2Database()
    cursor = connection.cursor()

    # create posts table
    query = f"""
        CREATE TABLE IF NOT EXISTS {config.analysisTableName} 
            (
                id STRING NOT NULL UNIQUE,
                author STRING, 
                creationDate TIMESTAMP, 
                language STRING,
                text TEXT,
                nonHateful FLOAT,
                racism FLOAT,
                religious FLOAT,
                sexism FLOAT,
                sexualOrientation FLOAT
            )
    """ 
    cursor.execute(query)

    # create index
    #cursor.execute(f"CREATE INDEX idx_addedToDB ON {config.analysisTableName}(addedToDB)")


def extractPrediction(analysisJSON):

    # Named Tuple to contain data row
    Prediction = namedtuple('Prediction', ['nonHateful', 'racism', 'religious', 'sexism', 'sexualOrientation'])

    # remove trailing newline
    analysisJSONStripped = analysisJSON[:-2]

    # convert to Python dict
    analysis = json.loads(analysisJSONStripped)

    # extract fields
    prediction = analysis['prediction']
    nonHateful = prediction['Non HateFull']
    racism = prediction['Racism']
    religious  = prediction['Religious']
    sexism  = prediction['Sexism']
    sexualOrientation = prediction['Sexual orientation']

    # create prediction tuple
    prediction = Prediction(nonHateful, racism, religious, sexism, sexualOrientation)

    # return result
    return prediction

def addAnalysisRow(id, author, creationDate, language, text, nonHateful, racism, religious, sexism, sexualOrientation):

    connection = connect2Database()
    cursor = connection.cursor()

    query = f"""INSERT OR IGNORE INTO {config.analysisTableName} (id, author, creationDate, language, text, nonHateful, racism, religious, sexism, sexualOrientation) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    
    try:
        valueTuple = (id, author, creationDate, language, text, nonHateful, racism, religious, sexism, sexualOrientation)
        cursor.execute(query, valueTuple)
        connection.commit()
    
    except sqlite3.InterfaceError as e:
        print("Error while adding analysis point")
        print(e)
        print(f"query: {query}")
        print(valueTuple)
        
    connection.close()


def importCSV2DB():
    
    # making data frame from csv file
    data = pd.read_csv(config.csvFilename)

    for tuple in data.itertuples():
        (index, id, author, creationDate, language, text, analysisJSON) = tuple

        p = extractPrediction(analysisJSON)
        
        addAnalysisRow(id=id, author=author, creationDate=creationDate, language=language, text=text,
                       nonHateful=p.nonHateful, racism=p.racism, religious=p.religious, sexism=p.sexism, sexualOrientation=p.sexualOrientation)

        sys.stdout.write(f"\r{index}")
        sys.stdout.flush()

        #break

if __name__ == '__main__':
    
    #dropDatabase()
    #createDatabase()
    #importCSV2DB()


