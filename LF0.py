import json
import boto3

'''
When the API receives a request, you should
1. extract the text message from the API request,
2. send it to your Lex chatbot,
3. wait for the response,
4. send back the response from Lex as the API response.
'''

def parser(lex_input, userId):

    msg = lex_input['message']
    status = lex_input["ResponseMetadata"]["HTTPStatusCode"]

    if status == 200:
        messages = {
                "messages": [{
                    "type": "unstructured",
                    "unstructured":{
                        "id": userId,
                        "text": msg
                    }
                }]
            }
        response = {
            "statusCode": 200,
            "headers":{
                "Content-Type":"application/json",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
            },
            "body": json.dumps(messages)
        }
    else:
        messages = {"messages": []}
        response = {
            "statusCode": status,
            "headers":{
                "Content-Type":"application/json",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
            },
            "body": json.dumps(messages)
        }
    print(response)
    return response

def lambda_handler(event, context):

    text = json.loads(event['body'])['messages'][0]['unstructured']['text']
    userId = event['requestContext']['accountId']

    client = boto3.client('lex-runtime')
    lex_input = client.post_text(botName='AI_Concierge', botAlias='test', userId=userId, inputText=text)

    return parser(lex_input, userId)
