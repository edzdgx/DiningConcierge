import json
from datetime import datetime
import boto3

client = boto3.client('dynamodb')

with open("Brazilian5.json") as jsonFile:
    jsonObject = json.load(jsonFile)
    jsonFile.close()




# data = dict()
# for i in range(len(jsonObject['data']['search'])):
#     jsonObject['data']['search']['business'][i]["insertedAtTimestamp"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

for i in range(len(jsonObject['data']['search']['business'])):
    data = dict()
    data['business_id'] = {"S": jsonObject['data']['search']['business'][i]["id"]}
    data['name'] = {"S": jsonObject['data']['search']['business'][i]["name"]}
    data["address"] = {"S": jsonObject['data']['search']['business'][i]["location"]['address1']}
    data['coordinates'] = {"M": {"latitude": {"N": str(jsonObject['data']['search']['business'][i]["coordinates"]["latitude"])},"longitude": {"N": str(jsonObject['data']['search']['business'][i]["coordinates"]["longitude"])}}}
    data['number_of_reviews'] = {"N": str(jsonObject['data']['search']['business'][i]["rating"])}
    data['rating'] = {"N": str(jsonObject['data']['search']['business'][i]["rating"])}
    data['zip_code'] = {"S": jsonObject['data']['search']['business'][i]["location"]["postal_code"]}
    data["categories"] = {"S": "Brazilian"}
    data["insertedAtTimestamp"] = {"S": datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    response = client.put_item(
        TableName='yelp-restaurants',
        Item=data)
    print(response)









data = jsonObject['data']['search']['business']


print(data)