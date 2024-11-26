import json
import requests
import boto3
import os
import urllib
import datetime
import time

client = boto3.client('lambda')

TRADE_TEMPLATE = """
	"blocks": [
		{
			"type": "section",
			"block_id": "team_1",
			"text": {
				"type": "mrkdwn",
				"text": "Team 1 Players"
			},
			"accessory": {
				"type": "multi_external_select",
				"placeholder": {
					"type": "plain_text",
					"text": "Select options",
					"emoji": true
				},
				"action_id": "player-search-action-1"
			}
		},
		{
			"type": "section",
			"block_id": "team_2",
			"text": {
				"type": "mrkdwn",
				"text": "Team 2 Players"
			},
			"accessory": {
				"type": "multi_external_select",
				"placeholder": {
					"type": "plain_text",
					"text": "Select options",
					"emoji": true
				},
				"action_id": "player-search-action-2"
			}
		},
		{
			"type": "input",
			"block_id": "league_number",
			"element": {
				"type": "plain_text_input",
				"action_id": "plain_text_input-action"
			},
			"label": {
				"type": "plain_text",
				"text": "League Number",
				"emoji": false
			}
		}
        <loantemplatecontent>
	]
"""

LOAN_TEMPLATE = """
,
		{
			"type": "input",
			"block_id": "loan_type",
			"element": {
				"type": "radio_buttons",
				"initial_option": {
				    "text": {
							"type": "plain_text",
							"text": "Full Loan",
							"emoji": true
						},
						"value": "full-loan",
				},
				"options": [
					{
						"text": {
							"type": "plain_text",
							"text": "Full Loan",
							"emoji": true
						},
						"value": "full-loan",
						
					},
					{
						"text": {
							"type": "plain_text",
							"text": "No Loan",
							"emoji": true
						},
						"value": "no-loan"
					},
					{
						"text": {
							"type": "plain_text",
							"text": "Other",
							"emoji": true
						},
						"value": "partial-loan"
					}
				],
				"action_id": "checkboxes-action"
			},
			"label": {
				"type": "plain_text",
				"text": "Loan amount",
				"emoji": true
			}
		},
		{
			"type": "input",
			"block_id": "partial_loan",
			"element": {
				"type": "plain_text_input",
				"action_id": "plain_text_input-action"
			},
			"label": {
				"type": "plain_text",
				"text": "Partial loan amount",
				"emoji": true
			},
			"optional": true
		}
"""

VIEW_TEMPLATE = """
    {
        "type": "modal",
        "callback_id": <callbackid>,
        "private_metadata": <metadata>,
        "title": {
            "type": "plain_text",
            "text": "<title>",
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
        <blocks>
    }
    """

SHOW_PLAYER_TEMPLATE = """
"blocks": [ 
                    { 
                        "type": "section", 
                        "text": { 
                            "type": "mrkdwn", 
                            "text": "*Choose player information to post*" 
                        } 
                    }, 
                    { 
                        "type": "section", 
                        "text": { 
                            "type": "mrkdwn", 
                            "text": "Player:" 
                        }, 
                        "block_id": "player_block",
                        "accessory": { 
                            "type": "static_select", 
                            "placeholder": { 
                                "type": "plain_text", 
                                "text": "Select player" 
                            }, 
                            "options": [ 
                                <player_text> 
                            ], 
                            "initial_option": <initial_option>,
                            "action_id": "player_selection_action" 
                        } 
                    }, 
                    { 
                        "type": "section", 
                        "text": { 
                            "type": "mrkdwn", 
                            "text": "Format:" 
                        }, 
                        "block_id": "format_block",
                        "accessory": { 
                            "type": "static_select", 
                            "placeholder": { 
                                "type": "plain_text", 
                                "text": "Select scoring format" 
                            }, 
                            "initial_option": { 
                                    "text": { 
                                        "type": "plain_text", 
                                        "text": "FanGraphs Points" 
                                    }, 
                                    "value": "3" 
                                },
                            "options": [ 
                                { 
                                    "text": { 
                                        "type": "plain_text", 
                                        "text": "FanGraphs Points" 
                                    }, 
                                    "value": "3" 
                                }, 
                                { 
                                    "text": { 
                                        "type": "plain_text", 
                                        "text": "SABR Points" 
                                    }, 
                                    "value": "4" 
                                }, 
                                { 
                                    "text": { 
                                        "type": "plain_text", 
                                        "text": "Classic 4x4" 
                                    }, 
                                    "value": "1" 
                                }, 
                                { 
                                    "text": { 
                                        "type": "plain_text", 
                                        "text": "Old School 5x5" 
                                    }, 
                                    "value": "2" 
                                } 
                            ], 
                            "action_id": "format_select_action" 
                        } 
                    }, 
                    { 
                        "type": "input",
            			"label": {
            				"type": "plain_text",
            				"text": "Select pages to link"
            			},
            			"block_id": "link_block",
            			"element": {
                            "type": "checkboxes", 
                            "options": [ 
                                { 
                                    "text": { 
                                        "type": "mrkdwn", 
                                        "text": "*Ottoneu Player Page*" 
                                    }, 
                                    "value": "ottoneu" 
                                }, 
                                { 
                                    "text": { 
                                        "type": "mrkdwn", 
                                        "text": "*FanGraphs Player Page*" 
                                    }, 
                                    "value": "fg" 
                                }, 
                                { 
                                    "text": { 
                                        "type": "mrkdwn", 
                                        "text": "*StatCast Player Page*" 
                                    }, 
                                    "value": "sc" 
                                }
                            ], 
                            "initial_options": [{ 
                                    "text": { 
                                        "type": "mrkdwn", 
                                        "text": "*Ottoneu Player Page*" 
                                    }, 
                                    "value": "ottoneu" 
                                }],
                            "action_id": "checkboxes-action" 
                        } 
                    }
                ]
"""


def lambda_handler(event, context):
    try:
        print(event)

        if event.get('Records', None):
            msg_map = list()
            for record in event['Records']:
                msg_map.append(json.loads(json.loads(record['Sns']['Message']))['default'])
        else:
            msg_map = [event]

        for record in msg_map:
            try:
                print(record)

                if 'ts' in record:
                    add_voting(record)
                    continue

                if record['command'].startswith('/link-player'):
                    show_player(record)

                if record['command'].startswith('/trade-review'):
                    show_trade_window(record)
            except Exception as e:
                print(e)
    except Exception as e:
        print(e)

    return {'statusCode': 200}


def add_voting(msg_map):
    payload = json.loads(msg_map['payload'])
    metadata = payload['view']['private_metadata'].split(',')

    header = {'Content-Type': 'application/x-www-form-urlencoded'}

    react_dict = dict()
    react_dict['channel'] = metadata[2]
    react_dict['token'] = os.environ[f'{msg_map["stage"]}_{metadata[3]}_token']
    react_dict['timestamp'] = msg_map['ts']

    react_dict['name'] = 'one'
    response = requests.post(
        'https://slack.com/api/reactions.add',
        headers=header,
        data=urllib.parse.urlencode(react_dict),
    )

    print('sleeping')
    time.sleep(1.5)
    print('awake')

    react_dict['name'] = 'scales'
    response = requests.post(
        'https://slack.com/api/reactions.add',
        headers=header,
        data=urllib.parse.urlencode(react_dict),
    )

    time.sleep(1.5)

    react_dict['name'] = 'two'
    response = requests.post(
        'https://slack.com/api/reactions.add',
        headers=header,
        data=urllib.parse.urlencode(react_dict),
    )

    print(response.content)

    return {'statusCode': 200}


def mongo_client_warm(msg_map):
    search_parameters = {'league_id': '160'}

    search_version = os.environ[f'{msg_map["stage"]}_search_version']

    _ = client.invoke(
        FunctionName=os.environ['player_search_lambda_arn'],
        InvocationType='RequestResponse',
        Payload=json.dumps(search_parameters),
        Qualifier=search_version,
    )


def show_trade_window(msg_map):
    print('in trade windwo')
    # Warm MongoClient
    mongo_client_warm(msg_map)
    view = create_view(msg_map, TRADE_TEMPLATE, 'Trade Review Wizard')
    update_res = update_view(msg_map, view)
    print(update_res)

    return {'statusCode': 200}


def show_player(msg_map):
    search_parameters = {'search_name': msg_map['text'], 'stage': msg_map['stage']}

    search_version = os.environ[f'{msg_map["stage"]}_search_version']

    response = client.invoke(
        FunctionName=os.environ['player_search_lambda_arn'],
        InvocationType='RequestResponse',
        Payload=json.dumps(search_parameters),
        Qualifier=search_version,
    )

    lambda_response = json.load(response['Payload'])

    if 'body' in lambda_response:
        player_list = json.loads(lambda_response['body'])
    else:
        player_list = None

    if player_list:
        blocks = get_modal_response_block_show_players(player_list)
    else:
        blocks = get_empty_player_list_blocks(msg_map['text'])

    view = create_view(msg_map, blocks, 'Player Search Wizard')
    update_res = update_view(msg_map, view)

    return {'statusCode': 200}


def update_view(msg_map, view):
    print('updating view')
    post_url = 'https://slack.com/api/views.update'
    data = urllib.parse.urlencode(
        {
            'view': view,
            'view_id': msg_map['view_id'],
            'token': os.environ[f'{msg_map["stage"]}_{msg_map["team_id"]}_token'],
        }
    )
    data = data.encode('utf-8')
    request = urllib.request.Request(post_url, data=data, method='POST')
    request.add_header('Content-Type', 'application/x-www-form-urlencoded')
    res = urllib.request.urlopen(request).read().decode('utf-8')
    return res


def create_view(msg_map, blocks, title):
    view = VIEW_TEMPLATE
    view = view.replace('<callbackid>', '"' + msg_map['trigger_id'] + '"')
    view = view.replace(
        '<metadata>',
        '"'
        + msg_map['command']
        + ','
        + msg_map['response_url']
        + ','
        + msg_map['channel_id']
        + ','
        + msg_map['team_id']
        + '"',
    )
    view = view.replace('<blocks>', blocks)
    view = view.replace('<title>', title)
    now_month = datetime.datetime.now().month
    if now_month < 2 or now_month > 9:
        loan_temp = ''
    else:
        loan_temp = LOAN_TEMPLATE
    view = view.replace('<loantemplatecontent>', loan_temp)
    return view


def get_empty_player_list_blocks(name):
    blocks = """
            "blocks": [
                        {
                            "type": "header",
                            "text": {
                                "type": "plain_text",
                                "text": "No players found for <name>, try again"
                            }
                        }
                    ]
            """
    return blocks.replace('<name>', '"' + name + '"')


def get_modal_response_block_show_players(player_list):
    text_player_list = []
    for player_dict in player_list:
        name = f"{player_dict['name']}, {player_dict['positions']}, {player_dict['org']}"
        if 'fg_majorleagueid' in player_dict:
            id = f'{player_dict['ottoneu_id']},{player_dict['fg_majorleagueid']}'
        else:
            id = f'{player_dict['ottoneu_id']},{player_dict['fg_minorleagueid']}'
        if 'mlbam_id' in player_dict and player_dict['mlbam_id'] != '0':
            id += f',{player_dict['mlbam_id']}'
        text_player_list.append(f'{{ \
						"text": {{ \
							"type": "plain_text", \
							"text": "{name}" \
                        }}, \
						"value": "{id}" \
					}}')
    player_text = ','.join(text_player_list)
    blocks = SHOW_PLAYER_TEMPLATE
    blocks = blocks.replace('<initial_option>', text_player_list[0])
    return blocks.replace('<player_text>', player_text)
