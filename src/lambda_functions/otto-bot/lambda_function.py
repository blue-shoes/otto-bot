import json
import urllib
from urllib import parse as urlparse
import base64
import boto3
import requests
import os

client = boto3.client('lambda')
sqs = boto3.client('sqs')
valid_commands = ['/link-player', '/trade-review']
loading_commands = ['/link-player', '/trade-review']

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
            if command.startswith('/link-player'):
                return link_player_result(payload, msg_map, metadata)
            if command.startswith('/trade-review'):
                return trade_review_result(payload, msg_map, metadata)
            return {
                'statusCode': 400,
                'body': json.dumps(f'Not a valid slash command: {msg_map.get('command', None)}')
            }
    
    valid_command = False
    input_command = msg_map.get('command', None)
    for command in valid_commands:
        if input_command and input_command.startswith(command):
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
        if input_command and input_command.startswith(command):
            modal_res = initiate_loading_modal(msg_map)
            print(modal_res)
            if modal_res['ok']:
                msg_map['view_id'] = modal_res['view']['id']
            else:
                print(modal_res)
                return {
                'statusCode': 400, 
                'body': json.dumps('Error creating interactive dialog')
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

def trade_review_result(payload, msg_map, metadata):
    vals = payload['view']['state']['values']
    
    selected_format = vals['format_block']['format_select_action']['selected_option']['value']
    
    league_id = vals['league_number']['plain_text_input-action']['value']
    
    loan_type = vals['loan_type']['checkboxes-action']['selected_option']['value']
    partial_loan_amount = vals['partial_loan']['plain_text_input-action']['value']
    
    if selected_format == '1':
        text_response = 'Scoring: 4x4'
    elif selected_format == '2':
        text_response = 'Scoring: 5x5'
    elif selected_format == '3':
        text_response = 'Scoring: FGP'
    elif selected_format == '4':
        text_response = 'Scoring: SABR'
    elif selected_format == '5':
        text_response = 'Scoring: FGP H2H'
    elif selected_format == '6':
        text_response = 'Scoring: SABR H2H'
    else:
        return {
            'statusCode': 400
        }
        
    if loan_type == 'full-loan':
        text_response += ', Full Loan'
    elif loan_type == 'no-loan':
        text_response += ', No Loan'
    
    text_response += '\n:one:\n'
    
    team_1_players_options = vals['team_1']['player-search-action-1']['selected_options']
    #team_1_player_ids = (option['value'] for option in team_1_players_options)
    
    text_response += '\n'.join(f'<https://ottoneu.fangraphs.com/playercard/{option['value']}/{selected_format}|{option["text"]["text"]}>' for option in team_1_players_options)
        
    
    team_2_players_options = vals['team_2']['player-search-action-2']['selected_options']
    #team_2_player_ids = (option['value'] for option in team_2_players_options)
    
    text_response += '\n:two:\n'
    
    text_response += '\n'.join(f'<https://ottoneu.fangraphs.com/playercard/{option['value']}/{selected_format}|{option["text"]["text"]}>' for option in team_2_players_options)
    
    if loan_type == 'partial-loan':
        text_response += '\nPartial loan: {partial_loan_amount}'
    
    response_dict = {}
    response_dict['response_type'] = 'in_channel'
    response_dict['text'] = text_response
    response_dict['channel'] = metadata[2]
    response_dict['token']  = os.environ[f'{msg_map["stage"]}_{metadata[3]}_token']
    response_dict['unfurl_links'] = False
    
    header = {'Content-Type': 'application/x-www-form-urlencoded'}

    response = requests.post('https://slack.com/api/chat.postMessage', headers=header, data=urllib.parse.urlencode(response_dict))
    
    ts = json.loads(response.content.decode('utf-8'))['ts']
    
    react_dict = dict()
    react_dict['channel'] = metadata[2]
    react_dict['token']  = os.environ[f'{msg_map["stage"]}_{metadata[3]}_token']
    react_dict['timestamp'] = ts
    
    react_dict['name'] = 'one'
    response = requests.post('https://slack.com/api/reactions.add', headers=header, data=urllib.parse.urlencode(react_dict))
    
    react_dict['name'] = 'two'
    response = requests.post('https://slack.com/api/reactions.add', headers=header, data=urllib.parse.urlencode(react_dict))
    
    react_dict['name'] = 'scales'
    response = requests.post('https://slack.com/api/reactions.add', headers=header, data=urllib.parse.urlencode(react_dict))
    
    print(response.content)
    
    return {
        'statusCode': 200
    }

def link_player_result(payload, msg_map, metadata):
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
                    "text": "Loading..."
                }
            }
        ]
    }
    """