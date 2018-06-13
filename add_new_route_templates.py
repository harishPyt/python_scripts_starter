from pymongo import MongoClient
import logging
logging.basicConfig(filename='../addNewRoute.log',level=logging.DEBUG)
# Connect to a database.
def connectToDb(uri, dbName):
    client = MongoClient(uri)
    db = client[dbName]
    return db

#Returns a specific collection.
def getACollection(db, collectionName):
    return db[collectionName]

# Checks if a route template is already created for the city
def checkIfRoutePresent(city, db):
    rome2rio = getACollection(db, 'rome2rioResponses')
    route = rome2rio.find_one({"fromCity.planningid": city["planningid"]})
    if route!=None and "routes" in "routes" not in list(route["response"].keys()):
        return True
    else:
        return False
# Gets all European cities.
def getAllEuropeanCities(db):
    """Returns all European cities present in the database."""
    region = getACollection(db, 'searchregion')
    europeanCountries = region.find_one({"regionCode": "eur"}, {"countryIds": 1})["countryIds"]
    country = getACollection(db, 'country')
    europeanCountriesData = country.find({"countryId": {"$in": europeanCountries}})
    countryCodes = []
    for country in europeanCountriesData:
        countryCodes.append(country["countryCode"])
    city = getACollection(db, 'city')
    europeanCities = city.find({"countryCode": {"$in": countryCodes}})
    return europeanCities
# Write array to collection
def write_to_db(db, arr):
    r2r = getACollection(db, 'rome2rioResponses')
    result = r2r.insert_many(arr)
    try:
        assert len(result.inserted_ids) == len(arr)
    except AssertionError:
        print("There is a mis-match in the number of documents inserted", len(result.inserted_ids), len(arr))
    return None

db=connectToDb("mongodb://oceanjardb:oceanjardbwwmib3112#@35.154.159.75:27017/oceanjar?authMechanism=MONGODB-CR", "oceanjar")
europeanCities = getAllEuropeanCities(db)
europeanCitiesMap = {}
routeNotPresentCities = []
for city in europeanCities:
    europeanCitiesMap[city["planningid"]] = city
    isRoutePresent = checkIfRoutePresent(city, db)
    if isRoutePresent!=True:
        routeNotPresentCities.append(city)
defaultResponseTemplates=[]
for city1 in routeNotPresentCities and len(routeNotPresentCities) > 0:
    for city2 in list(europeanCitiesMap.keys()):
        if city1!=city2:
            responseTemplate1 = {
                "fromCity": europeanCitiesMap[city1],
                "toCity": europeanCitiesMap[city2],
                "response": {}
            }
            responseTemplate2 = {
                "fromCity": europeanCitiesMap[city2],
                "toCity": europeanCitiesMap[city1],
                "response": {}
            }
            defaultResponseTemplates.append(responseTemplate1)
            defaultResponseTemplates.append(responseTemplate2)
if len(routeNotPresentCities) > 0:
    write_to_db(db, defaultResponseTemplates)



