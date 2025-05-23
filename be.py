from flask import Flask, jsonify, request
from elasticsearch import Elasticsearch
import requests
app = Flask(__name__)


eshost = "https://umbc-search-9870646635.us-east-1.bonsaisearch.net:443/processed/_doc/_search"

@app.route('/')
def hello_geek():
    return '<h1>Hello from Flask & Docker</h2>'


@app.route('/aggregations', methods=['GET'])
def get_aggregations():
    search_query = request.args.get('q')
    query = {
        "size": 0,
        "aggs": {
            "sentiment_count": {
                "terms": {
                    "field": "sentiment.keyword"
                }
            },
            "location_count": {
                "terms": {
                    "field": "location.keyword"
                }
            },
            "hashtags_count": {
                "terms": {
                    "field": "hashtags.keyword",
                    "size": 10
                }
            },
            "tier_count": {
                "terms": {
                    "field": "tier.keyword"
                }
            }
        }
    }

    if search_query:
        query["query"] = {
            "multi_match": {
                "query": search_query,
                "fields": ["renderedContent", "sentiment", "tier", "hashtags"]
            }
        }

    print(query)
    # Execute the Elasticsearch query
    response = requests.post(eshost, json=query,  auth=("gqwgzb0w1d", "ztnumhbhdu"))

    print(response.text)
    # Parse and format the response
    aggregations = {}
    formatted_response = {
        "stats": {},
        "demographics": [],
        "hashtags_count": [],
        "location_count": []
    }
    if response.status_code == 200:
        data = response.json()

        for agg_name, agg_data in data['aggregations'].items():
            if agg_name == 'sentiment_count':
                for bucket in agg_data['buckets']:
                    formatted_response["stats"][f"#{bucket['key'].lower()}"] = bucket['doc_count']
            elif agg_name == 'location_count':
                for bucket in agg_data['buckets']:
                    formatted_response["location_count"].append({f"#{bucket['key']}": bucket['doc_count']})
            elif agg_name == 'hashtags_count':
                for bucket in agg_data['buckets']:
                    formatted_response["hashtags_count"].append({f"#{bucket['key']}": bucket['doc_count']})
            elif agg_name == 'tier_count':
                for bucket in agg_data['buckets']:
                    formatted_response["demographics"].append({f"#{bucket['key']}": bucket['doc_count']})
            else:
                formatted_response[agg_name] = agg_data['buckets']
    else:
        return  jsonify(formatted_response)

    return jsonify(formatted_response)



if __name__ == "__main__":
    app.run(debug=True)