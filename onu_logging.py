import json

logBuffer = ""

def __init__(self, arg):
    super(OnuLogging, self).__init__()
    self.arg = arg

def logMessage(message):
    global logBuffer
    print message
    logBuffer += "<p>" + message + "</p>"

def logErrorMessage(message, lineNumber):
    logMessage("Line " + str(lineNumber))
    logMessage(message)

def logJsonMessage(message, jsonObj): 
    jsonString = json.dumps(jsonObj, sort_keys=True,
                            indent=4, separators=(',', ': '))
    logMessage(message)
    logMessage(jsonString)

def returnLog():
    global logBuffer
    log = logBuffer
    return log
