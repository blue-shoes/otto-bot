import json
import urllib
from urllib import parse as urlparse
import base64
import boto3
import requests
import os

client = boto3.client('lambda')
sqs = boto3.client('sqs')
valid_commands = ['/link-player']
loading_commands = ['/link-player']

def lambda_handler(event, context):
    
    msg_map = dict(urlparse.parse_qsl(base64.b64decode(str(event['body'])).decode('ascii')))
    
    print(event['requestContext'])
    
    msg_map['stage'] = event['requestContext']['stage']
    
    print(msg_map)
    
    if msg_map.get('payload', None):
        payload = json.loads(msg_map['payload'])
        if payload['type'] == 'block_actions':
            return {
                'statusCode': 200
            }
        elif payload['type'] == 'view_submission':
            metadata = payload['view']['private_metadata'].split(',') 
            command = metadata[0]
            if command == '/link-player' or command == '/link-player-dev':
                vals = payload['view']['state']['values']
                selected_player = vals['player_block']['player_selection_action']['selected_option']
                player_text = selected_player['text']['text']
                name_split = player_text.split(',')
                ids = selected_player['value'].split(',')
                link_types = [sel['value'] for sel in vals['link_block']['checkboxes-action']['selected_options']]

                if not link_types:
                    print(msg_map['payload'])
                    return {
                              "response_action": "errors",
                              "errors": {
                                    "link_block": "Must select at least one linkage"
                               }
                            }

                if 'ottoneu' in link_types:

                    selected_format = vals['format_block']['format_select_action']['selected_option']['value']
    
                    otto_player_link = f'https://ottoneu.fangraphs.com/playercard/{ids[0]}/{selected_format}'
        
                    text_response = f'<{otto_player_link}|{name_split[0]}> {", ".join(name_split[1:])}'
                    
                    if 'fg' in link_types:
                        fg_link = f'http://www.fangraphs.com/statss.aspx?playerid={ids[1]}'
                        text_response += f' (<{fg_link}|FG>)'
                    
                    if 'sc' in link_types:
                        sc_link = f'https://baseballsavant.mlb.com/savant-player/{ids[2]}'
                        text_response += f' (<{sc_link}|SC>)'
                
                elif 'fg' in link_types:
                    fg_link = f'http://www.fangraphs.com/statss.aspx?playerid={ids[1]}'
                    text_response = f'<{fg_link}|{name_split[0]}> {", ".join(name_split[1:])}'
                    
                    if 'sc' in link_types:
                        sc_link = f'https://baseballsavant.mlb.com/savant-player/{ids[2]}'
                        text_response += f' (<{sc_link}|SC>)'
                
                elif 'sc' in link_types:
                    sc_link = f'https://baseballsavant.mlb.com/savant-player/{ids[2]}'
                    text_response = f'<{sc_link}|{name_split[0]}> {", ".join(name_split[1:])}'
                
                else:
                    return {
                        'statusCode': 400,
                        'body': json.dumps(f'Invalid link type(s) "{link_types}" selected".')
                    }

                response_dict = {}
                response_dict['response_type'] = 'in_channel'
                response_dict['text'] = text_response
                
                header = {'Content-Type': 'application/json'}
                response = requests.post(metadata[1], headers=header, data=json.dumps(response_dict))

                return {
                    'statusCode': 200
                }
    
    valid_command = False
    input_command = msg_map.get('command', None)
    for command in valid_commands:
        if input_command and command.startswith(command):
            valid_command = True
            break
    
    if not valid_command:
        print(event)
        print(msg_map)
        return {
            'statusCode': 400,
            'body': json.dumps(f'Not a valid slash command: {msg_map.get('command', None)}')
        }
    
    #if not msg_map.get('text', None):
    #    return {
    #        'statusCode': 400, 
    #        'body': json.dumps(f'No arguments given for slash command: {msg_map['command']}')
    #    }
    
    for command in loading_commands:
        if input_command and command.startswith(command):
            modal_res = initiate_loading_modal(msg_map)
            print(modal_res)
            if modal_res['ok']:
                msg_map['view_id'] = modal_res['view']['id']
            else:
                print(modal_res)
                return {
                'statusCode': 400, 
                'body': json.dumps(f'Error creating interactive dialog')
            }
    
    queueurl = sqs.get_queue_url(QueueName='Otto-bot-queue')['QueueUrl']
    try:
        sqs.send_message(QueueUrl=queueurl, MessageBody=json.dumps(msg_map))
    except:
        return {
            'statusCode': 400, 
            'body': json.dumps(f'Error when submitting to the queue.')
        }
    
    return {
        'statusCode': 202
    }

def initiate_loading_modal(msg_map):
    post_url = 'https://slack.com/api/views.open'
    view = get_modal()
    view = view.replace('<callbackid>', '"' + msg_map['trigger_id'] + '"')
    data = urllib.parse.urlencode({
        "trigger_id": msg_map['trigger_id'],
        "view": view,
        "token": os.environ[f'{msg_map["stage"]}_{msg_map["team_id"]}_token']
    })
    data = data.encode("utf-8")
    request = urllib.request.Request(post_url, data=data, method="POST")
    request.add_header("Content-Type", "application/x-www-form-urlencoded")
    res = json.loads(urllib.request.urlopen(request).read().decode('utf-8'))
    return res

def get_modal():
    return """
    {
        "type": "modal",
        "callback_id": <callbackid>,
        "title": {
            "type": "plain_text",
            "text": "Otto-bot Wizard",
            "emoji": true
        },
        "submit": {
            "type": "plain_text",
            "text": "Submit",
            "emoji": true
        },
        "close": {
            "type": "plain_text",
            "text": "Cancel",
            "emoji": true
        },
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "Loading players..."
                }
            }
        ]
    }
    """