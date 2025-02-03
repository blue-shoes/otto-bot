import json
import urllib
from urllib import parse as urlparse
import base64
import boto3
import requests
import os

lambda_client = boto3.client('lambda')
sns = boto3.client('sns')

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
            return {'statusCode': 200}
        elif payload['type'] == 'view_submission':
            metadata = payload['view']['private_metadata'].split(',')
            command = metadata[0]
            if command.startswith('/link-player'):
                return link_player_result(payload, msg_map, metadata)
            if command.startswith('/trade-review'):
                return trade_review_result(payload, msg_map, metadata)
            return {
                'statusCode': 400,
                'body': json.dumps(f'Not a valid slash command: {msg_map.get('command', None)}'),
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
            'body': json.dumps(f'Not a valid slash command: {msg_map.get('command', None)}'),
        }

    # if not msg_map.get('text', None):
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
                    'body': json.dumps('Error creating interactive dialog'),
                }

    target_arn = os.environ['ottobot_sns_arn']
    result = sns.publish(TargetArn=target_arn, Message=json.dumps({'default': json.dumps(msg_map)}))

    print(result)

    return {'statusCode': 202}


def trade_review_result(payload, msg_map, metadata):
    vals = payload['view']['state']['values']

    # selected_format = vals['format_block']['format_select_action']['selected_option']['value']

    league_id = vals['league_number']['plain_text_input-action']['value']

    opl = len(vals['opl_trade']['opl-checkboxes-action']['selected_options']) > 0

    search_parameters = {'league_id': league_id}

    search_version = os.environ[f'{msg_map["stage"]}_league_load_version']

    response = lambda_client.invoke(
        FunctionName=os.environ['league_load_lambda_arn'],
        InvocationType='RequestResponse',
        Payload=json.dumps(search_parameters),
        Qualifier=search_version,
    )

    lambda_response = json.load(response['Payload'])

    if 'body' in lambda_response:
        league_info = json.loads(lambda_response['body'])
        print(league_info)
        player_dict = league_info['rosters']
        league_format = league_info['format']
    else:
        player_dict = None

    if not player_dict:
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
            },
            'body': json.dumps(
                {
                    'response_action': 'errors',
                    'errors': {'league_number': f'League Id {league_id} not available in database.'},
                }
            ),
        }

    if 'loan_type' in vals:
        loan_type = vals['loan_type']['checkboxes-action']['selected_option']['value']
        if loan_type == 'partial-loan':
            try:
                partial_loan_amount = vals['partial_loan']['plain_text_input-action']['value']
                if partial_loan_amount[0] == '$':
                    partial_loan_amount = partial_loan_amount[1:]
                _ = int(partial_loan_amount)
            except ValueError:
                return {
                    'statusCode': 200,
                    'headers': {
                        'Content-Type': 'application/json',
                    },
                    'body': json.dumps(
                        {
                            'response_action': 'errors',
                            'errors': {'partial_loan': 'Enter custom loan as only number of dollars (e.g. "10")'},
                        }
                    ),
                }
        else:
            partial_loan_amount = None
    else:
        loan_type = 'off-season'
        partial_loan_amount = None

    if opl:
        league_format = 3
        text_response = 'Scoring: OPL'
    elif league_format == 1:
        text_response = 'Scoring: 4x4'
    elif league_format == 2:
        text_response = 'Scoring: 5x5'
    elif league_format == 3:
        text_response = 'Scoring: FGP'
    elif league_format == 4:
        text_response = 'Scoring: SABR'
    elif league_format == 5:
        text_response = 'Scoring: FGP H2H'
    elif league_format == 6:
        text_response = 'Scoring: SABR H2H'
    else:
        print(f'Invalid format {league_format}.')
        return {
            'statusCode': 400,
            'body': json.dumps(f'Invalid format {league_format}.'),
        }

    team_1_players_options = vals['team_1']['player-search-action-1']['selected_options']
    team_2_players_options = vals['team_2']['player-search-action-2']['selected_options']

    if not team_1_players_options and not team_2_players_options:
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
            },
            'body': json.dumps(
                {
                    'response_action': 'errors',
                    'errors': {'league_number': 'Please select at least one player for the trade.'},
                }
            ),
        }

    team_1_salaries = 0
    team_1_names = list()
    for option in team_1_players_options:
        rostered = player_dict.get(option['value'], None)
        if not rostered:
            print('not_rostered 1')
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                },
                'body': json.dumps(
                    {
                        'response_action': 'errors',
                        'errors': {
                            'league_number': f'{option["text"]["text"].split(", ")[0]} is not rostered in league {league_id}'
                        },
                    }
                ),
            }
        salary = rostered.get('Salary')
        team_1_salaries += int(salary.split('$')[1])
        team_1_names.append((option['value'], salary, option['text']['text'].split(', ')))

    team_2_salaries = 0
    team_2_names = list()
    for option in team_2_players_options:
        rostered = player_dict.get(option['value'])
        if not rostered:
            print('not_rostered 2')
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                },
                'body': json.dumps(
                    {
                        'response_action': 'errors',
                        'errors': {
                            'league_number': f'{option["text"]["text"].split(", ")[0]} is not rostered in league {league_id}'
                        },
                    }
                ),
            }
        salary = rostered.get('Salary')
        team_2_salaries += int(salary.split('$')[1])
        team_2_names.append((option['value'], salary, option['text']['text'].split(', ')))

    team_1_more_salary = team_1_salaries > team_2_salaries
    salary_diff = abs(team_1_salaries - team_2_salaries)

    if partial_loan_amount and int(partial_loan_amount) < 0:
        team_1_more_salary = not team_1_more_salary

    text_response += '\n:one:  '

    if team_1_names:
        text_response += '\n\t\t'.join(
            f'{option[1]} <https://ottoneu.fangraphs.com/playercard/{option[0]}/{league_format}|{option[2][0]}> {", ".join(option[2][1:])}'
            for option in team_1_names
        )
    else:
        text_response += 'None'

    if team_1_more_salary:
        if loan_type == 'full-loan':
            text_response += f'\n\t\tFull Loan (${salary_diff})'
        elif partial_loan_amount:
            net_int = salary_diff - abs(int(partial_loan_amount))
            if net_int < 0:
                net = f'-${abs(net_int)}'
            else:
                net = f'${net_int}'
            text_response += f'\n\t\t${abs(int(partial_loan_amount))} Loan (Net {net})'
        elif loan_type == 'off-season':
            if salary_diff == 0:
                text_response += '\n\t\tNet $0'
            else:
                text_response += f'\n\t\tNet -${salary_diff}'
        else:
            if salary_diff == 0:
                text_response += '\n\t\tNo Loan (Net $0)'
            else:
                text_response += f'\n\t\tNo Loan (Net -${salary_diff})'

    text_response += '\n:two:  '

    if team_2_names:
        text_response += '\n\t\t'.join(
            f'{option[1]} <https://ottoneu.fangraphs.com/playercard/{option[0]}/{league_format}|{option[2][0]}> {", ".join(option[2][1:])}'
            for option in team_2_names
        )
    else:
        text_response += 'None'

    if not team_1_more_salary:
        if loan_type == 'full-loan':
            text_response += f'\n\t\tFull Loan (${salary_diff})'
        elif partial_loan_amount:
            net_int = salary_diff - abs(int(partial_loan_amount))
            if net_int < 0:
                net = f'-${abs(net_int)}'
            else:
                net = f'${net_int}'
            text_response += f'\n\t\t${abs(int(partial_loan_amount))} Loan (Net {net})'
        elif loan_type == 'off-season':
            if salary_diff == 0:
                text_response += '\n\t\tNet $0'
            else:
                text_response += f'\n\t\tNet -${salary_diff}'
        else:
            if salary_diff == 0:
                text_response += '\n\t\tNo Loan (Net $0)'
            else:
                text_response += f'\n\t\tNo Loan (Net -${salary_diff})'

    response_dict = {}
    response_dict['response_type'] = 'in_channel'
    response_dict['text'] = text_response
    response_dict['channel'] = metadata[2]
    response_dict['token'] = os.environ[f'{msg_map["stage"]}_{metadata[3]}_token']
    response_dict['unfurl_links'] = False

    header = {'Content-Type': 'application/x-www-form-urlencoded'}

    response = requests.post(
        'https://slack.com/api/chat.postMessage',
        headers=header,
        data=urllib.parse.urlencode(response_dict),
    )

    print(response.content)

    ts = json.loads(response.content.decode('utf-8'))['ts']

    msg_map['ts'] = ts

    target_arn = os.environ['ottobot_sns_arn']
    _ = sns.publish(TargetArn=target_arn, Message=json.dumps({'default': json.dumps(msg_map)}))

    return {'statusCode': 200}


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
            'response_action': 'errors',
            'errors': {'link_block': 'Must select at least one linkage'},
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
            'body': json.dumps(f'Invalid link type(s) "{link_types}" selected".'),
        }

    response_dict = {}
    response_dict['response_type'] = 'in_channel'
    response_dict['text'] = text_response

    header = {'Content-Type': 'application/json'}
    _ = requests.post(metadata[1], headers=header, data=json.dumps(response_dict))

    return {'statusCode': 200}


def initiate_loading_modal(msg_map):
    post_url = 'https://slack.com/api/views.open'
    view = get_modal()
    view = view.replace('<callbackid>', '"' + msg_map['trigger_id'] + '"')
    data = urllib.parse.urlencode(
        {
            'trigger_id': msg_map['trigger_id'],
            'view': view,
            'token': os.environ[f'{msg_map["stage"]}_{msg_map["team_id"]}_token'],
        }
    )
    data = data.encode('utf-8')
    request = urllib.request.Request(post_url, data=data, method='POST')
    request.add_header('Content-Type', 'application/x-www-form-urlencoded')
    res = json.loads(urllib.request.urlopen(request).read().decode('utf-8'))
    return res


def get_modal():
    return """
    {
        "type": "modal",
        "callback_id": <callbackid>,
        "title": {
            "type": "plain_text",
            "text": "Otto-bot",
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
