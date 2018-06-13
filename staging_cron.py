from pymongo import MongoClient
import requests as req
import logging
logging.basicConfig(filename='../cronlog.log',level=logging.DEBUG)
# Connect to a database.
def connectToDb(uri, dbName):
    client = MongoClient(uri)
    db = client[dbName]
    return db

#Returns a specific collection.
def getACollection(db, collectionName):
    return db[collectionName]

# Rome2Rio - api call and parsing functions

#Get a rome2Rio route and return it as a json
def callRome2Rio(oName, dName, oPosLat, oPosLong, dPosLat, dPosLong):
    orgCo = str(oPosLat)+","+str(oPosLong)
    destCo = str(dPosLat)+","+str(dPosLong)
    url = 'http://free.rome2rio.com/api/1.4/json/Search?key=IRlABFW8&oName='+oName+'&dName='+dName+'&oPos='+orgCo+'&dPos='+destCo
    retryCount = 0
    re = req.get(url)
    data = {}
    if re.status_code==200:
        data = re.json()
    else:
        print("Something went wrong. The status we got is ", re.status_code)
        retryPass = False
        while retryPass==False and retryCount < 100:
            retryCount+=1
            print("Trying for the ",retryCount," time")
            re = req.get(url)
            if(re.status_code==200):
                retryPass = True
                data = re.json()
            if(re.status_code==444):
                retryPass=True
                print("Wrong destination name")
                data = {}
            if(re.status_code==429):
                retryPass=True
                print("Too-Many requests per hour")
            if(re.status_code==402):
                retryPass=True
                print("Payment Required")
    print("Got data in ",retryCount," retry/retries")  
    return data

def update_response(fromCityId, toCityId, responseTobeUpdated, collection):
    collection.update({"fromCity.planningid": fromCityId, "toCity.planningid": toCityId}, {"$set": {"response": responseTobeUpdated}})
    return None



db=connectToDb("mongodb://oceanjardb:oceanjardbwwmib3112#@35.154.159.75:27017/oceanjar?authMechanism=MONGODB-CR", "oceanjar")
rome2Rio = getACollection(db, 'rome2rioResponses')
allResponses = rome2Rio.find()
count = 0
for resp in allResponses:
    if "routes" not in list(resp["response"].keys()):
        # I need to call rome2rio
        r2rResp = callRome2Rio(resp["fromCity"]["name"], resp["toCity"]["name"], resp["fromCity"]["latitude"], resp["fromCity"]["longitude"], resp["toCity"]["latitude"], resp["toCity"]["longitude"])
        update_response(resp["fromCity"]["planningid"], resp["toCity"]["planningid"], r2rResp, rome2Rio)
        logging.debug("The response is updated for the route %s and %s",resp["fromCity"]["name"], resp["toCity"]["name"])
        count+=1
    if count==300:
        break