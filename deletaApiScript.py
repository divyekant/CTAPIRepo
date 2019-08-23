import csv
from datetime import date
import requests
import json


def deleteAPI(accountID, accountPassCode, data):
    url = 'https://api.clevertap.com/1/delete/profiles.json'

    headers = {
        'X-CleverTap-Account-Id': '%s' % accountID,
        'X-CleverTap-Passcode': '%s' % accountPassCode,
        'Content-Type': 'application/json; charset=utf-8',
    }

    response = requests.request("POST", url, headers=headers, data=json.dumps(data))

    if response.status_code != 200:
        return 0
    else:
        res = response.json()
        if res["status"] != "success":
            return 0
        return 1

def addlinebreak(filename):
    with open(filename, 'a') as csvfile:
        csvfile.write("\n")
    csvfile.close()


def writetocsv(filename, data):
    with open(filename, 'a') as csvfile:
        json.dump(data, csvfile)

    csvfile.close()

def callDeleteAPI(accountID, accountPassCode, data,retryfile):
    retryFlag = True
    retryCount = 0

    while retryFlag and retryCount <= 3:
        res = deleteAPI(accountID, accountPassCode, data)
        if res == 0:
            retryFlag = True
            retryCount = retryCount + 1
            print "Retrying"
        else:
            retryFlag = False

    if retryCount > 3 and retryFlag == True:
        writetocsv(retryfile, data)
        addlinebreak(retryfile)



def mainActivity():
    print "Make sure file and script are in the same folder! The csv should have only one column with no headers. [Test on min 100 profiles]"
    accountID = raw_input("Enter Account ID:")
    accountPassCode = raw_input("Enter Account PassCode:")
    filename = raw_input("Enter CSV Filename [eg - myfile.csv]:")
    identifier = raw_input("Enter identifier [identity/objectid]:")

    retryfile = "ErrorFile.json"

    if identifier == "identity":
        i = "identity"
    else:
        i = "guid"

    print "Reading from file: %s" % filename

    with open(filename, 'r') as csvFile:

        objectCounter = 1
        data = {i: []}

        while True:
            row = csvFile.readline()

            if row == "":
                callDeleteAPI(accountID, accountPassCode, data, retryfile)

                break

            data[i].append(str(row.strip("\n")))

            if objectCounter == 100:
                callDeleteAPI(accountID, accountPassCode, data, retryfile)

                objectCounter = 0
                data = {i: []}

            objectCounter = objectCounter + 1

        print "Closing CSV file"

        csvFile.close()

    print "Dunzo."


mainActivity()