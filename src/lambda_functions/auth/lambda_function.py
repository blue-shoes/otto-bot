import json
import urllib.parse
import urllib.request
import os

def lambda_handler(event, context):
    print(event)
    data = urllib.parse.urlencode({
            "client_id": os.environ["CLIENT_ID"],
            "client_secret": os.environ["CLIENT_SECRET"],
            "code": event["queryStringParameters"]["code"]
        })
    data = data.encode("ascii")
    
    request = urllib.request.Request("https://slack.com/api/oauth.v2.access", data=data, method="POST")
    request.add_header( "Content-Type", "application/x-www-form-urlencoded" )
    response = urllib.request.urlopen(request).read()
    response = response.decode("utf-8")
    response = json.loads(response)
    print(f'team_id: {response["team"]["id"]}')
    print(f'token: {response["access_token"]}')
    return {
        'statusCode': 200,
        'body': 'OK',
        "headers": {
            'Content-Type': 'text/html',
        }
    }

