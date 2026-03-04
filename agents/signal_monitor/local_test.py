import json
from lambda_handler import lambda_handler

test_event = {
    'messageVersion': '1.0',
    'agent': {'name': 'sentinel-signal-monitor'},
    'actionGroup': 'gdelt-query-action',
    'apiPath': '/query-gdelt',
    'httpMethod': 'POST',
    'requestBody': {
        'content': {
            'application/json': {
                'properties': [
                    {
                        'name': 'keywords',
                        'type': 'string',
                        'value': '["attack", "military", "killed", "forces"]'
                    },

                ]
            }
        }
    }
}

result = lambda_handler(test_event, {})
print('Status:', result['response']['httpStatusCode'])
body = json.loads(result['response']['responseBody']['application/json']['body'])
print('Total events:', body['query']['total_found'])
print('Keywords used:', body['query']['keywords_used'])
if body['query']['total_found'] > 0:
    print('First event:')
    print(json.dumps(body['events'][0], indent=2))
if 'error' in body:
    print('Error:', body['error'])