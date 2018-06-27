from pprint import pprint

from tabulate import tabulate

from bson.son import SON
from collections import OrderedDict
import pymongo
from pymongo import MongoClient

LIMIT = 10
connection = MongoClient()
database = connection.python_bugs_db
collection = database.python_bugs_1

def print_table(data, headers):
    print(tabulate(data, headers, tablefmt='pipe'))

def main():
    print("\nTop issues by message \n")

    print_table(list(collection
                     .aggregate([{"$unwind": "$content"},
                                 {"$group": {"_id": "$title", "count": {"$sum": 1}}},
                                 {'$project':{ 'title':'$_id', 'message_count':'$count', "_id": False}},
                                 {"$sort": SON([("message_count", -1)])},
                                 {"$limit": LIMIT}])), headers="keys")

    print("\nMessage count distribution \n")

    print_table(list(collection
                       .aggregate([{"$unwind": "$content"},
                                   {"$group": {"_id": "$title", "count": {"$sum": 1}}},
                                   {"$group": {"_id": "$count", "count": {"$sum": 1}}},
                                   {"$sort": SON([("count", -1)])},
                                   {"$limit": LIMIT}])), headers="keys")

    print("\nTop authors by message count \n")

    print_table(list(collection
                        .aggregate([
                            {"$unwind": "$content"},
                            {"$group": {"_id": "$content.author", "count": {"$sum": 1}}},
                            {'$project':{ 'author':'$_id', 'count':'$count', "_id": False}},
                            {"$sort": SON([("count", -1)])},
                            {"$limit": LIMIT}])), headers="keys")

    print("\nTop python versions by issue count \n")

    print_table(list(collection
                        .aggregate([
                            {"$unwind": "$version"},
                            {"$group": {"_id": "$version", "count": {"$sum": 1}}},
                            {'$project':{ 'version':'$_id', 'count':'$count', "_id": False}},
                            {"$sort": SON([("count", -1)])},
                            {"$limit": LIMIT}])), headers="keys")

    print("\Total issues \n")

    print(collection
          .aggregate([
              {"$group": {"_id": "$title", "count": {"$sum": 1}}},
              {"$sort": SON([("count", -1)])}]).count())


    connection.close()

if __name__ == "__main__":
    main()
