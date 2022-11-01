import base64
import json
from datetime import datetime
from uuid import uuid4

import boto3

dynamodb_client = boto3.client("dynamodb")
s3_client = boto3.client("s3")


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
    article_id = event["queryStringParameters"]["articleId"]
    response = dynamodb_client.get_item(
        TableName="articles",
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
    save_article_file(article["file"], article_id)

    return {"articleId": article_id}


def get_random_articles(event):
    response = dynamodb_client.scan(
        TableName="articles",
        Limit=20,
        Select="ALL_ATTRIBUTES",
    )

    return [parse_dynamodb_article(article) for article in response["Items"]]


def save_article_record(article, article_id, creation_date):
    dynamodb_client.put_item(
        TableName="articles",
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
            "creation_date": {
                "S": creation_date
            }
        }
    )


def save_article_file(base64_file, article_id):
    base64_bytes = base64_file.encode("ascii")
    file_bytes = base64.b64decode(base64_bytes)
    file = file_bytes.decode("ascii")
    s3_client.put_object(Body=file, Bucket="ronainc-articles", Key=f"{article_id}.html")


def parse_dynamodb_article(dynamodb_article):
    article = {
        "id": dynamodb_article.get("id").get("S"),
        "category": dynamodb_article.get("category").get("S"),
        "abstract": dynamodb_article.get("abstract").get("S"),
        "title": dynamodb_article.get("title").get("S"),
        "creationDate": dynamodb_article.get("creation_date").get("S"),
    }

    return article


def parse_dynamodb_attribute(attribute, attribute_name, attribute_type):
    if attribute:
        attribute = attribute.get(attribute_name).get(attribute_type)

    return attribute


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
