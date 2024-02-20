import json
import requests
import boto3
import os
import urllib

client = boto3.client('lambda')

VIEW_TEMPLATE = """
    {
        "type": "modal",
        "callback_id": <callbackid>,
        "private_metadata": <metadata>,
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
    
    if 'command' in event:
        msg_map = event
    else:
        msg_map = json.loads(event['Records'][0]['body'])
    
    print(msg_map)
    
    if msg_map['command'].startswith('/link-player'):
        return show_player(msg_map)
    
    return {
        'statusCode': 404,
        'body': json.dumps('Not a valid command')
    }

def show_player(msg_map):

    search_parameters = {
        "search_name" : msg_map['text']
    }
    
    search_version = os.environ[f'{msg_map["stage"]}_search_version']
    
    response = client.invoke(
        FunctionName = os.environ['player_search_lambda_arn'],
        InvocationType = 'RequestResponse',
        Payload = json.dumps(search_parameters),
        Qualifier = search_version
    )
    
    lambda_response = json.load(response['Payload'])
    
    player_list = json.loads(lambda_response['body'])

    if player_list:
        blocks = get_modal_response_block_show_players(player_list)
    else:
        blocks = get_empty_player_list_blocks(msg_map['text']) 
    
    view = create_view(msg_map, blocks)
    update_res = update_view(msg_map, view)
    
    return {
        'statusCode': 200
    }

def update_view(msg_map, view):
    post_url = 'https://slack.com/api/views.update'
    data = urllib.parse.urlencode({
        "view": view,
        "view_id": msg_map['view_id'],
        "token": os.environ[f'{msg_map["stage"]}_{msg_map["team_id"]}_token']
    })
    data = data.encode("utf-8")
    request = urllib.request.Request(post_url, data=data, method="POST")
    request.add_header("Content-Type", "application/x-www-form-urlencoded")
    res = urllib.request.urlopen(request).read().decode('utf-8')
    return res

def create_view(msg_map, blocks):
    view = VIEW_TEMPLATE
    view = view.replace('<callbackid>', '"' + msg_map['trigger_id'] + '"')
    view = view.replace('<metadata>', '"' + msg_map['command'] +"," + msg_map['response_url'] + '"')
    view = view.replace('<blocks>', blocks)
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
        if player_dict['fg_majorleagueid']:
            id = f'{player_dict['ottoneu_id']},{player_dict['fg_majorleagueid']}'
        else:
            id = f'{player_dict['ottoneu_id']},{player_dict['fg_minorleagueid']}'
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
