import requests
import csv

import time


def queryCall(eventName,accid,accpc,dfrom,dto,trendType):
    url = "https://api.clevertap.com/1/counts/trends.json"

    payload = "{\"event_name\":\"%s\",\"from\":%s,\"to\":%s,\"unique\":false,\"groups\":{\"foo\":{\"trend_type\":\"%s\"}}}" % (eventName,dfrom,dto,trendType)
    headers = {
        'X-CleverTap-Account-Id': "%s" % accid,
        'X-CleverTap-Passcode': "%s" % accpc,
        'Content-Type': "application/json",
        'cache-control': "no-cache",
        'Postman-Token': "687db015-6665-4a5b-9706-69f78b7a03e5"
        }

    print "Making the Trends Api Call for Event: " + eventName

    response = requests.request("POST", url, data=payload, headers=headers)

    if response.status_code != 200:
        return 0
    else:
        res = response.json()
        if res["status"] == "partial":
            reqid = res["req_id"]
            return reqid
        else:
            return 0

def partialCall(reqid,accid,accpc):
    url = "https://api.clevertap.com/1/counts/trends.json"

    querystring = {"req_id": "%s" % reqid}

    payload = ""
    headers = {
        'X-CleverTap-Account-Id':  "%s" % accid,
        'X-CleverTap-Passcode': "%s" % accpc,
        'Content-Type': "application/json",
        'cache-control': "no-cache",
        'Postman-Token': "21b84377-9fc4-4709-9836-65f13452160d"
    }

    print "Making the Trends Api Request ID Call"

    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)


    if response.status_code != 200:
        return 0
    else:
        res = response.json()
        if res["status"]=="partial":
            return 0
        return res


def writetocsv(filename,row):
    with open('%s' % filename, 'a' ) as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        spamwriter.writerow(row)
    csvfile.close()


def createcsv(filename):
    with open('%s' % filename, 'w+') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
    csvfile.close()


def main():
    csvfilename = raw_input("Enter CSV file name : ")
    orgName = raw_input("Enter org name : ")
    accid = raw_input("Enter Account ID : ")
    accpcode = raw_input("Enter Account Passcode : ")
    trendType = raw_input("Enter Trend Type [Monthly,Weekly,Daily] : ")
    dfrom = raw_input("Enter From Date [YYYYMMDD] : ")
    dto = raw_input("Enter To Date [YYYYMMDD] : ")


    fileName = "Count for %s" % orgName

    createcsv(fileName)


    with open('%s' % csvfilename, 'rb') as csvfile:
        spamreader = csv.reader(csvfile)
        ftFlag = True
        for event in spamreader:
            eventName = event[0]
            print "Doing for Event: " + eventName

            row = []
            headers = []
            row.append(eventName)
            headers.append("Name")

            #  get req id
            reqID = queryCall(eventName,accid,accpcode,dfrom,dto,trendType)

            while reqID == 0:
                time.sleep(5)
                reqID = queryCall(eventName, accid, accpcode,dfrom,dto,trendType)


            retryFlag = True
            retryCount = 0


            while retryFlag and retryCount <=10:
                res = partialCall(reqID,accid,accpcode)
                if res == 0:
                    retryFlag = True
                    time.sleep(5)
                    retryCount = retryCount + 1
                else:
                    retryFlag = False


            if retryCount <=10:
                if res["status"] == "success":
                    data = res["foo"]
                    for eachdate in data:
                        if ftFlag:
                            headers.append(eachdate)
                        row.append(data[eachdate])
                    if ftFlag:
                        writetocsv(fileName,headers)
                        ftFlag = False

                    writetocsv(fileName,row)

                else:
                    print "Non - Success Status returned "
                    print res
            else:
                print "Too Many retries for Event: " + eventName

            print "Done for Event: " + eventName
            time.sleep(5)

    print "Done"

main()









