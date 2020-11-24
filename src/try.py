from datetime import datetime

def currentbatchtime(ti: str) -> int:
    # dtime = datetime.strptime("2016-04-15T08:27:18-0500", "%Y-%m-%dT%H:%M:%S%z")
    # print(datetime.strptime("2016-04-15T08:27:18-0500", "%Y-%m-%dT%H:%M:%S%z"))
    epoch = datetime(1970, 1, 1)
    dtime = datetime.strptime(ti, "%Y-%m-%dT%H:%M:%S")
    return int((dtime - epoch).total_seconds())*1000
    
# print(currentbatchtime('2016-04-15T08:27:18'))

b = datetime(2020, 11, 20, 7, 3, 10, 0)
print(b)
# print(currentbatchtime('2016-04-15T08:27:18'))