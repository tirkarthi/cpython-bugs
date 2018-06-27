import re
import glob
import json
from pymongo import MongoClient

def main():
    json_files = glob.glob("data/*json")

    client = MongoClient()
    db = client.python_bugs_db
    collection = db.python_bugs_1
    collection.drop()
    items = []

    ISSUE_REGEX = re.compile("issue<\d+>")
    TITLE_REGEX = re.compile("<title>(.*)</title>", re.I | re.M | re.S)

    for item in json_files:
        with open(item) as f:
            content = f.read()
            issue = json.loads(content)
            message_count = issue.get("message_count")
            # if message_count != "None"and message_count:
            #     issue["message_count"] = int(float(message_count))
            match = ISSUE_REGEX.search(item)
            if match:
                issue['issue_id'] = match.group(0)
            with open(item.replace(".json", "")) as f:
                content = f.read()
                match = TITLE_REGEX.search(content)
                if match:
                    issue['title'] = match.group(1).strip().replace("\n", "")
            items.append(issue)
        if len(items) > 1000:
            collection.insert_many(items)
            items = []

    if items:
        collection = db.python_bugs
        collection.insert_many(items)

    client.close()

if __name__ == "__main__":
    main()

'''
Top 10 commented issues

db.python_bugs.find({"message_count": {$ne : "None"}}, {"title": 1, "id": 1, "activity": 1, "message_count": 1, "_id": 0}).sort({message_count : -1}).limit(10)

Message count distribution

db.python_bugs.aggregate({"$group": {"_id": "$message_count", count: {"$sum" : 1}}}, {$sort: {count: -1}})

Average message count

db.python_bugs.aggregate({"$group": {"_id": null, "avg_msg_count": {"$avg" : "$message_count"}}})

'''
