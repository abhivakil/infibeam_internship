"""
Access details for datastores being used
"""

# PostgreSQL
class PSQLDetails():
    psql = {
        "user": "tmshszilbcculx",
        "password": "ee95204861668778837467651b39614a5529edd88412c1fe0a8b9a21419bf437",
        "host": "ec2-54-246-185-161.eu-west-1.compute.amazonaws.com",
        "port": "5432",
        "database": "dd83caln9k71ig"
    }


# MongoDB
class MongoDBDetails():
    conn_string = "mongodb://admin:8VGFjzL879y2tydc@ac-cvfgt3j-shard-00-00.c25uga7.mongodb.net:27017,ac-cvfgt3j-shard-00-01.c25uga7.mongodb.net:27017,ac-cvfgt3j-shard-00-02.c25uga7.mongodb.net:27017/?ssl=true&replicaSet=atlas-f6fwku-shard-0&authSource=admin&retryWrites=true&w=majority"
    

