import csv
import json

def csv_to_json(csv_file):
    tweets = []
    with open(csv_file, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            tweet = {
                "url": row["url"],
                "date": row["date"],
                "content": row["content"],
                "renderedContent": row["renderedContent"],
                "id": int(row["id"]),
                "user": {
                    "username": row["user.username"],
                    "displayname": row["user.displayname"],
                    "id": int(row["user.id"]),
                    "description": row["user.description"],
                    "rawDescription": row["user.rawDescription"],
                    "verified": bool(row["user.verified"]),
                    "created": row["user.created"],
                    "followersCount": int(row["user.followersCount"]),
                    "friendsCount": int(row["user.friendsCount"]),
                    "statusesCount": int(row["user.statusesCount"]),
                    "favouritesCount": int(row["user.favouritesCount"]),
                    "listedCount": int(row["user.listedCount"]),
                    "mediaCount": int(row["user.mediaCount"]),
                    "location": row["user.location"],
                    "protected": bool(row["user.protected"]),
                    "profileImageUrl": row["user.profileImageUrl"],
                    "profileBannerUrl": row["user.profileBannerUrl"],
                    "url": row["user.url"]
                },
                "outlinks": [],
                "tcooutlinks": [],
                "replyCount": int(row["replyCount"]),
                "retweetCount": int(row["retweetCount"]),
                "likeCount": int(row["likeCount"]),
                "quoteCount": int(row["quoteCount"]),
                "conversationId": int(row["conversationId"]),
                "lang": row["lang"],
                "source": row["source"],
                "sourceUrl": row["sourceUrl"],
                "sourceLabel": row["sourceLabel"],
                "media": None if row["media"] == "null" else [{
                    "previewUrl": row["media.previewUrl"],
                    "fullUrl": row["media.fullUrl"],
                    "type": row["media.type"]
                }],
                "retweetedTweet": None if row["retweetedTweet"] == "null" else json.loads(row["retweetedTweet"]),
                "quotedTweet": None if row["quotedTweet"] == "null" else json.loads(row["quotedTweet"]),
                "mentionedUsers": None if row["mentionedUsers"] == "null" else json.loads(row["mentionedUsers"])
            }
            tweets.append(tweet)
    return tweets

csv_file = "tweets.csv"
tweets_json = csv_to_json(csv_file)
print(json.dumps(tweets_json, indent=4))
