from decouple import config

MONGO_URL = config("MONGO_URL", default="")
ELASTIC_SEARCH = config("ELASTIC_SEARCH", default="")
