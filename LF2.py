import json
import boto3
from botocore.vendored import requests
import random
import gzip

'''
Purpose of this Lambda function:
1. pulls a message from the SQS queue (Q1),
2. gets a random restaurant recommendation for the cuisine collected through conversation from ElasticSearch and DynamoDB,
3. formats restaurant recommendation
4. sends them over text message to the phone number included in the SQS message, using SNS
'''
def get_message_attribute(message, attribute):
    return message['MessageAttributes'][attribute]['StringValue']

def get_message():
    client = boto3.client('sqs')
    queueUrl = 'https://sqs.us-east-1.amazonaws.com/108057842050/MyQueue'
    response = client.receive_message(
        QueueUrl=queueUrl,
        AttributeNames=[
            'All'
        ],
        MessageAttributeNames=[
            'All',
        ],
        VisibilityTimeout=10,
        WaitTimeSeconds=0
    )
    if "Messages" in response.keys():
        return response['Messages']
    else:
        return []

def delete_message(receiptHandle):
    client = boto3.client('sqs')
    queueUrl = 'https://sqs.us-east-1.amazonaws.com/108057842050/MyQueue'
    response = client.delete_message(
        QueueUrl=queueUrl,
        ReceiptHandle=receiptHandle
    )

def recommendation_from_opensearch(categories):
    baseUrl = 'https://search-cc-restaurant-i5r7ahptz75kqm36erw4b4j4gy.us-east-1.es.amazonaws.com'
    auth = ('master', 'password')
    headers = {'Content-Type': 'application/json'}
    query = {
        "size": 200,
        "query": {
            "match": {
                "categories": categories
            }
        }
    }
    path = '/restaurants/_search'
    url = baseUrl + path
    response = requests.get(url, auth=auth, headers=headers, data=json.dumps(query))
    return json.loads(response.text)

def parse_recommendation(recommendation):
    # get a random restaurant_id as recommendation
    business_ids = []
    for _ in range(3):
        r = random.randint(0, 199)
        business_ids.append(recommendation['hits']['hits'][r]["_source"]["business_id"])
    return business_ids


def recommendation_details(business_ids):
    # queryDynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurants')
    restaurant_detail = []
    for business_id in business_ids:
        restaurant_detail.append(table.get_item(Key={'business_id': business_id})['Item'])
    return restaurant_detail

def push_to_sns(Data):
    # parse restaurant info and use SNS to send info to user via message
    topic_arn = 'arn:aws:sns:us-east-1:108057842050:restaurantRecommendation'

    client = boto3.client('sns')

    message = 'Thank you for waiting, after careful research, Here is my restaurant recommendation: \n1.\nRestaurant name: {}\nAddress: {}\nRating: {} \n2.\nRestaurant name: {}\nAddress: {}\nRating: {}\n3.\nRestaurant name: {}\nAddress: {}\nRating: {}\n\nBon appetit' \
        .format(Data[0]['name'], Data[0]['address'], Data[0]['rating'],Data[1]['name'], Data[1]['address'], Data[1]['rating'],Data[2]['name'], Data[2]['address'], Data[2]['rating'])
    response = client.publish(
        TopicArn=topic_arn,
        Message=message,
        Subject='Restaurant Recommendation'
    )
    return response

def lambda_handler(event, context):
    messages = get_message()
    for message in messages:
        cuisine = get_message_attribute(message, attribute='Cuisine')
        recommendation = recommendation_from_opensearch(cuisine)
        business_id = parse_recommendation(recommendation)
        restaurant_details = recommendation_details(business_id)
        response = push_to_sns(restaurant_details)
        delete_message(message['ReceiptHandle'])
        return {
            "statusCode": 200,
            "body1":json.dumps(message),
            "body":cuisine,
            "body2":json.dumps(recommendation),
            "body3":business_id,
            "body4":restaurant_details,
            "body5":response
        }
