import pandas as pd
import json
import config
from collections import namedtuple

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

def importCSV2DB():
    
    # making data frame from csv file
    data = pd.read_csv(config.csvFilename)

    for tuple in data.itertuples():
        (index, id, author, creationDate, webUrl, text, analysisJSON) = tuple

        p = extractPrediction(analysisJSON)
        print(p.sexism)

        break

if __name__ == '__main__':
    
    importCSV2DB()


