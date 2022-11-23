import json
import os
from datetime import datetime
from uuid import uuid4

import boto3

dynamodb_client = boto3.client("dynamodb")

articles_table_name = os.getenv("ARTICLES_TABLE_NAME")


def lambda_handler(event, context):
    router = {
        "/article": {
            "POST": upload_article,
            "GET": get_article
        },
        "/article/random": {
            "GET": get_random_articles
        }
    }
    try:
        response_body = router[event["path"]][event["httpMethod"]](event)
    except Exception:
        return response(500, {"message": "Internal server error"})
    else:
        return response(200, response_body)


def get_article(event):
    article_id = event["queryStringParameters"]["id"]
    response = dynamodb_client.get_item(
        TableName=articles_table_name,
        Key={
            "id": {"S": article_id}
        }
    )

    return parse_dynamodb_article(response["Item"])


def upload_article(event):
    article = json.loads(event["body"])
    article_id = str(uuid4())
    now = datetime.now()
    save_article_record(article, article_id, now.strftime("%Y/%m/%d"))

    return {"articleId": article_id}


def get_random_articles(event):
    response = dynamodb_client.scan(
        TableName=articles_table_name,
        Limit=20,
        Select="ALL_ATTRIBUTES",
    )

    return [parse_dynamodb_article(article) for article in response["Items"]]


def save_article_record(article, article_id, creation_date):
    dynamodb_client.put_item(
        TableName=articles_table_name,
        Item={
            "id": {
                "S": article_id
            },
            "title": {
                "S": article.get("title")
            },
            "category": {
                "S": article.get("category")
            },
            "abstract": {
                "S": article.get("abstract")
            },
            "markdown": {
                "S": article.get("markdown")
            },
            "creation_date": {
                "S": creation_date
            }
        }
    )


def parse_dynamodb_article(dynamodb_article):
    article = {
        "id": dynamodb_article.get("id").get("S"),
        "title": dynamodb_article.get("title").get("S"),
        "category": dynamodb_article.get("category").get("S"),
        "abstract": dynamodb_article.get("abstract").get("S"),
        "markdown": dynamodb_article.get("markdown").get("S"),
        "creationDate": dynamodb_article.get("creation_date").get("S")
    }

    return article


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Headers": "Authorization,Content-Type,X-Api-Key",
            "Access-Control-Allow-Origin": "*"
        },
        "isBase64Encoded": False,
        "body": json.dumps(body)
    }
