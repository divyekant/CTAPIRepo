import csv
import requests
import json

objectIDListfilename = "objectIDListfilename.csv"

def getHeader(filelist):
    return filelist[0]


def getObjectIDs(header,row,accid,accpcode):
    url = "https://api.clevertap.com/1/profile.json"

    querystring = {header: "%s" % row}
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
        spamwriter = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        spamwriter.writerow(row)

def main():
    print "Welcome to SMS Unsub Script!"
    print "Steps:"
    print "1. Ensure file is in the same folder as this script"
    print "2. Ensure CSV file has header as per CT format [e.g - identity/easterobjectId]"
    print "3. Enter Account Details"
    print "You are good to go!"

    csvfilename = raw_input("Enter CSV file name : ")
    if not csvfilename.endswith(".csv"):
        print "Incorrect File Format"
    accid = raw_input("Enter Account ID : ")
    accpcode = raw_input("Enter Account Passcode : ")



    with open(csvfilename, "r") as csvfile:
        csvfilereader = csv.reader(csvfile)
        filelist = []
        for row in csvfilereader:
            if len(row) != 0:
                filelist = filelist + [row]

    csvfile.close()

    header = getHeader(filelist)

    writetocsv(objectIDListfilename,"")

    for row in filelist:
        row = row.strip(" ")
        objids = getObjectIDs(header,row,accid,accpcode)
        for objectId in objids:
            writetocsv(objectIDListfilename,objectId)

    payload = {"d": []}

    data = {
        header: "1",
        "type": "profile",
        "profileData": {
            "MSG-sms": False
        }
    }

    reader = csv.reader(open(objectIDListfilename, 'rb'))
    chunksize =  1000

    for i, line in enumerate(reader):
        if (i % chunksize == 0 and i > 0):
            uploadUnsubFlag(payload)
            payload = {"d": []}
        data[header] = line
        payload.append(line)

    print "Done"

main()