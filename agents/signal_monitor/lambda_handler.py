import json
from gdelt_query import run_query

SEED_KEYWORDS = ['conflict', 'tension', 'war', 'insurgency']


def lambda_handler(event, context):
    """
    AWS Lambda entry point for Bedrock Action Group.
    Translates Bedrock event format to run_query() call
    and wraps response in Bedrock's expected format.
    """
    action_group = event.get('actionGroup', '')
    api_path     = event.get('apiPath', '')
    http_method  = event.get('httpMethod', 'POST')

    try:
        if api_path == '/query-gdelt':
            result = handle_query_gdelt(event)
            status_code = 200
        else:
            result = {'error': f'Unknown apiPath: {api_path}'}
            status_code = 404

    except Exception as e:
        result = {'error': f'Handler error: {str(e)}'}
        status_code = 500

    return {
        'messageVersion': '1.0',
        'response': {
            'actionGroup': action_group,
            'apiPath': api_path,
            'httpMethod': http_method,
            'httpStatusCode': status_code,
            'responseBody': {
                'application/json': {
                    'body': json.dumps(result)
                }
            }
        }
    }


def handle_query_gdelt(event: dict) -> dict:
    """
    Extracts parameters from Bedrock event and calls run_query.
    """
    properties = (
        event
        .get('requestBody', {})
        .get('content', {})
        .get('application/json', {})
        .get('properties', [])
    )

    params = {p['name']: p['value'] for p in properties}

    raw_keywords = params.get('keywords', '[]')
    try:
        analyst_keywords = json.loads(raw_keywords)
        if isinstance(analyst_keywords, str):
            analyst_keywords = [analyst_keywords]
    except (json.JSONDecodeError, TypeError):
        analyst_keywords = [raw_keywords] if raw_keywords else []

    all_keywords = list(set(SEED_KEYWORDS + analyst_keywords))

    region = params.get('region', None)
    if region and region.lower() in ['none', 'null', '']:
        region = None

    return run_query(keywords=all_keywords, region=region)