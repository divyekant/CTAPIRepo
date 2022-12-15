import requests
import csv

import time

url = "https://api.clevertap.com/1/events.json"
# Enter CSV file name:
csvfilename = ""
# Enter org name :
orgName = ""
# Enter Account ID :
accid = ""
# Enter Account Passcode :
accpcode = ""
# Enter From Date [YYYYMMDD] :
dfrom = ""
# Enter To Date [YYYYMMDD] :
dto = ""
# Enter Event Name :
eventName = ""
# Add Headers [These are the event props] :
headers = []


def queryCall(eventName, accid, accpc, dfrom, dto):
    initialUrl = url + "?batch_size=5000"
    payload = "{\"event_name\":\"%s\",\"from\":%s,\"to\":%s}" % (eventName, dfrom, dto)
    headers = {
        'X-CleverTap-Account-Id': "%s" % accid,
        'X-CleverTap-Passcode': "%s" % accpc,
        'Content-Type': "application/json",
        'cache-control': "no-cache",
    }

    print("Making the Events Api Call for Event: " + eventName)

    response = requests.request("POST", initialUrl, data=payload, headers=headers)

    if response.status_code != 200:
        return 0
    else:
        res = response.json()
        if res["status"] == "success":
            if hasKey(res, "cursor"):
                cursor = res["cursor"]
                return cursor
            else:
                return 1
        else:
            return 0


def hasKey(obj, skey):
    try:
        for key in obj.keys():
            if skey == key:
                return True
    except:
        print(skey + " not found in " + obj)

    return False


def partialCall(cursor, accid, accpc):
    partialUrl = url + "?cursor=" + cursor

    payload = ""
    headers = {
        'X-CleverTap-Account-Id': "%s" % accid,
        'X-CleverTap-Passcode': "%s" % accpc,
        'Content-Type': "application/json",
        'cache-control': "no-cache",
        'Postman-Token': "21b84377-9fc4-4709-9836-65f13452160d"
    }

    print("Making the Events Api Cursor Call with cursor: " + cursor)

    response = requests.request("GET", partialUrl, data=payload, headers=headers)

    if response.status_code != 200:
        return 0, 0
    else:
        res = response.json()
        if res["status"] == "success":
            if hasKey(res, "next_cursor"):
                cursor = res["next_cursor"]
                return res, cursor
            else:
                return 1, 0
        return 0, 0


def writetocsv(filename, row):
    with open('%s' % filename, 'a') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        spamwriter.writerow(row)
    csvfile.close()


def createcsv(filename):
    with open('%s' % filename, 'w+') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
    csvfile.close()
    writetocsv(filename, headers)


def processRecord(record):
    try:
        f_dict = []

        event_props = record['event_props']
        for header in headers:
            f_dict.append(event_props.get(header, 0))

        return f_dict
    except:
        return False


def main():
    fileName = eventName + " for %s" % orgName

    createcsv(fileName)

    print("Doing for Event: " + eventName)

    #  get cursor
    cursor = queryCall(eventName, accid, accpcode, dfrom, dto)

    while cursor == 0:
        time.sleep(5)
        cursor = queryCall(eventName, accid, accpcode, dfrom, dto)

    # Get Cursor data
    retryFlag = True
    retryCount = 0

    while retryFlag and retryCount <= 10:
        res = partialCall(cursor, accid, accpcode)
        if res[0] == 0:
            retryFlag = True
            time.sleep(5)
            retryCount = retryCount + 1
        elif res[0] == 1:
            retryFlag = False
        else:
            retryFlag = False

    if retryCount <= 10:
        if res[0]["status"] == "success":
            records = res[0]["records"]
            for record in records:
                r = processRecord(record)
                writetocsv(fileName, r)

        else:
            print("Non - Success Status returned ")
            print(res)
    else:
        print("Too Many retries for Event: " + eventName)

    print("Done for Event: " + eventName)

    print("Done")


main()

