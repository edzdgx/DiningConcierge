import math
import dateutil.parser
import datetime
import time
import os
import logging
import boto3

'''
This Lambda function is built on the AWS OrderFlowersCodeHook Blueprint
'''

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# *************************************helpers**********************************************
def get_slots(intent_request):
    return intent_request['currentIntent']['slots']

def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }

def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')

def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False

def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot,
        }
    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }

def close(session_attributes, fulfillment_state, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }

# *****************************************************************************************

# basic validation
#  ~ make sure that the given time is at least 2 hours from the current time
#  ~ the given date is greater than equal to today's date
#  ~ the phone number should be a valid 10 digit number
#  ~ location is a valid location according to your data

def validate_reservation(location, cuisine, date, time, people, phone):
    cuisines = ['mexican', 'italian', 'french', 'british', 'chinese', 'brazilian', 'japanese']
    if location is not None and location.lower() != 'manhattan':
        # conduct business only in Manhattan
        return build_validation_result(False,
                                       'Location',
                                       'We do not have business in {}, would you like a different location?  '
                                       'Our most popular dining area is Manhattan'.format(location))

    if cuisine is not None and cuisine.lower() not in cuisines:
        # specified cuisine not in database
        return build_validation_result(False,
                                       'Cuisine',
                                       'We do not have {}, would you like a different type of cuisine?  '
                                       'Our most popular cuisine is Italian'.format(cuisine))

    if date is not None:
        if not isvalid_date(date):
            # Not a valid date
            return build_validation_result(False, 'Date', 'I did not understand that, what date would you like to make the reservation?')
        elif datetime.datetime.strptime(date, '%Y-%m-%d').date() < datetime.date.today():
            # Can't reserve before today
            return build_validation_result(False, 'Date', 'You can reserve table from today onwards. What date would you like to make the reservation?')

    if time is not None:
        if len(time) != 5:
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'Time', 'Please enter a valid time.')

        hour, minute = time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)

        if math.isnan(hour) or math.isnan(minute):
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'Time', 'Please enter a valid time.')
        if hour < 10 or hour > 22:
            # Outside of business hours
            return build_validation_result(False, 'Time', 'Our business hours are from 10 a m. to 10 p m. Can you specify a time during this range?')
        if datetime.datetime.strptime(date, '%Y-%m-%d').date() == datetime.date.today():
            # Reserve 2 hours before current time
            if hour - datetime.datetime.now().hour < 2:
                return build_validation_result(False, 'Time', 'Please reserve at least 2 hours from now')

    if people is not None:
        p = parse_int(people)
        if math.isnan(p):
            # Not a valid number of people
            return build_validation_result(False, 'NumOfPeople', 'Please enter a valid number for people.')
        elif float(people) != p or p <= 0:
            # Can't be a decimal or negative number
            return build_validation_result(False, 'NumOfPeople', 'Please enter a valid number for people.')

    if phone is not None:
        if not phone.isnumeric() or len(phone) != 10:
            return build_validation_result(False, 'PhoneNumber', 'Please enter a valid 10-digit phone number.')

    return build_validation_result(True, None, None)

def reservation(intent_request):
    # extract slots info from lex and check validity of user input
    slots = get_slots(intent_request)
    location = slots["Location"]
    cuisine = slots["Cuisine"]
    date = slots["Date"]
    time = slots["Time"]
    people = slots["NumOfPeople"]
    phone = slots["PhoneNumber"]
    source = intent_request['invocationSource']

    if source == 'DialogCodeHook':
        # Perform basic validation on the supplied input slots.
        validation_result = validate_reservation(location, cuisine, date, time, people, phone)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])

        output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}

        return delegate(output_session_attributes, slots)

    # push data to SQS
    push_to_sqs(slots)

    # call backend service: sqs, by pushing user reservation info to the backend
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'You’re all set. Expect my suggestions shortly! Have a good day.'})

def dispatch(intent_request):
    # Called when the user specifies an intent for this bot.
    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    # Dispatch to your bot's intent handlers
    if intent_name == 'DiningSuggestionIntent':
        return reservation(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')

def push_to_sqs(Data):
    # push user reservation info to sqs
    client = boto3.client('sqs')
    queueUrl = 'https://sqs.us-east-1.amazonaws.com/108057842050/MyQueue'
    messageAttributes = {
        "Location": {
            "DataType": "String",
            "StringValue": Data["Location"]
        },
        "Cuisine": {
            "DataType": "String",
            "StringValue": Data["Cuisine"]
        },
        "Date": {
            "DataType": "String",
            "StringValue": Data["Date"]
        },
        "Time": {
            "DataType": "String",
            "StringValue": Data["Time"]
        },
        "NumOfPeople": {
            "DataType": "String",
            "StringValue": Data["NumOfPeople"]
        },
        "PhoneNumberhone": {
            "DataType" : "String",
            "StringValue" : Data["PhoneNumber"]
        }
    }
    messageBody = "User reservation info"

    client.send_message(
        QueueUrl = queueUrl,
        MessageAttributes = messageAttributes,
        MessageBody = messageBody
    )

def lambda_handler(event, context):
    # main handler
    os.environ['TZ'] = 'America/New_York'
    time.tzset()

    return dispatch(event)
