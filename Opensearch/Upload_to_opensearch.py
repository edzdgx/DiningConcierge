import pandas
import json

df = pandas.read_csv('results_Brazilian.csv')

with open('Brazilian_json.json', 'w') as jsonFile:
     for i in range(len(df["business_id"])):
          dict1 = {"index":{"_index":"restaurants","_id": i+1435}}
          dict2 = {"business_id":df["business_id"][i],"categories":"Brazilian"}
          json.dump(dict1, jsonFile)
          jsonFile.write('\n')
          json.dump(dict2, jsonFile)
          jsonFile.write('\n')
     jsonFile.close()
















