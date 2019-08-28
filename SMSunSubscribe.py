import csv
import requests
import json

objectIDListfilename = "objectIDListfilename.csv"

def getHeader(filelist):
    return str(filelist[0][0])


def getObjectIDs(header,row,accid,accpcode):
    url = "https://api.clevertap.com/1/profile.json"

    querystring = {header: "%s" % row[0]}
    headers = {
        'content-type': "application/json",
        'x-clevertap-account-id': accid,
        'x-clevertap-passcode': accpcode,
    }

    response = requests.request("GET", url, headers=headers, params=querystring)

    if response.status_code != 200:
        return 0
    else:
        parsedjson = json.loads(response.text)
        objids=[]
        for j in parsedjson["record"]["platformInfo"]:
            objids.append(j["objectId"])
        return objids

def uploadUnsubFlag(batch,accid,accpcode):
    url = "https://api.clevertap.com/1/upload"
    payload = json.dumps(batch)
    headers = {
        'content-type': "application/json",
        'x-clevertap-account-id': accid,
        'x-clevertap-passcode': accpcode,
    }
    response = requests.request("POST", url, data=payload, headers=headers)
    if response.status_code != 200:
        return 0
    else:
        res = response.json()
        if res["status"] != "success":
            return 0
        return 1


def writetocsv(filename,row):
    with open('%s' % filename, 'a' ) as csvfile:
        spamwriter = csv.writer(csvfile)
        spamwriter.writerow([str(row).encode("utf-8")+""])
    csvfile.close()

def createCSVfile(filename):
    with open('%s' % filename, 'w+' ) as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
    csvfile.close()



def main():
    print "Welcome to SMS Unsub Script!"
    print "Steps:"
    print "1. Ensure file is in the same folder as this script"
    print "2. Ensure CSV file has header as per CT format [e.g - identity/email]"
    print "3. Enter Account Details"
    print "4. Enter which channel to Unsubscribe [Email/SMS/Push/WhatsApp]"
    print "You are good to go!"

    csvfilename = raw_input("Enter CSV file name : ")

    if not csvfilename.endswith(".csv"):
        print "Incorrect File Format"
        csvfilename = raw_input("Enter CSV file name : ")

    accid = raw_input("Enter Account ID : ")

    accpcode = raw_input("Enter Account Passcode : ")
    channel = raw_input("Enter channel [Email/SMS/Push/WhatsApp] : ")

    channel = channel.lower()

    print "Reading CSV file."
    with open(csvfilename, "r") as csvfile:
        csvfilereader = csv.reader(csvfile)
        filelist = []
        for row in csvfilereader:
            if len(row) != 0:
                filelist = filelist + [row]

    csvfile.close()

    header = getHeader(filelist)

    print "Creating ObjectID CSV file."

    createCSVfile(objectIDListfilename)

    isFirst = True

    for row in filelist:
        if isFirst == True:
            isFirst = False
            continue
        objids = getObjectIDs(header,row,accid,accpcode)
        for objectId in objids:
            writetocsv(objectIDListfilename,objectId)

    payload = {"d": []}

    field = "MSG-"+channel
    data = {
        "objectId": "1",
        "type": "profile",
        "profileData": {
            field: False
        }
    }

    batchsize = 1000

    print "Batch size set as: %s" % batchsize
    print "Reading ObjectID CSV file."

    batchcount = 0
    with open(objectIDListfilename, 'r') as csvFile:

        objectCounter = 1

        while True:
            row = csvFile.readline()

            if row == "":
                retryFlag = True
                retryCount = 0

                while retryFlag and retryCount <= 3:
                    res = uploadUnsubFlag(payload, accid, accpcode)
                    if res == 0:
                        retryFlag = True
                        retryCount = retryCount + 1
                    else:
                        retryFlag = False

                print "Done for last Batch"

                break

            data["objectId"] = (str(row.strip("\n"))).strip("\r")
            payload["d"].append(data)

            if objectCounter == batchsize:
                retryFlag = True
                retryCount = 0

                while retryFlag and retryCount <= 3:
                    res = uploadUnsubFlag(payload, accid, accpcode)
                    if res == 0:
                        retryFlag = True
                        retryCount = retryCount + 1
                    else:
                        retryFlag = False

                objectCounter = 0
                payload = {"d": []}
                batchcount = batchcount + 1

                print "Done for Batch No: %s" % batchcount

            objectCounter = objectCounter + 1
        csvFile.close()

    print "Done"

main()