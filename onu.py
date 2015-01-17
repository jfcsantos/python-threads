from datetime import datetime
import time
import sys
import threading
import json
import requests
from BeautifulSoup import BeautifulSoup
from pprint import pprint
from inspect import currentframe, getframeinfo
import onu_logging

class jobThread (threading.Thread):
    def __init__(self, threadLevel, parent, job, jobID, subJobID = None):
        threading.Thread.__init__(self)
        self.job = job
        self.jobID = jobID
        self.parent = parent
        self.subJobID = subJobID
        self.threadLevel = threadLevel        #set the threading level to handle thread number limitations

    def run(self):

        #Log the starting time of the thread
        self.timeStart = datetime.now()

        global globalConnCounter, runSingleJob
        jobID = self.jobID
        subJobID = self.subJobID

        if not subJobID is None:
            jobID  = self.subJobID

        #Use this var to decide if child thread can run according to parent statuses
        threadCanRun = True

        #Verify if this job was called by another job
        if self.parent > 0 and not stopThreads:

            try:
                #Check for multiple dependencies
                depArr = check_multiple_dependencies(jobID)
            except Exception as e:
                frameinfo = getframeinfo(currentframe())
                onu_logging.logErrorMessage("Could not load dependencies : %s" % e, frameinfo.lineno)


            #Wait for the parent job thread to finish
            self.parent.join()

            #Go through each parent job
            for parID in depArr:
                #If the parent hasn't finished yet we wait until it does so the child can proceed
                if int(parID) not in finishedJobs:
                    onu_logging.logMessage("Job "+str(jobID) + " waiting for parent: "+ str(parID) +" to finish")

                    while int(parID) not in finishedJobs and not stopThreads:
                        pass

                #If the parent has finished but without success we don't allow the child to proceed
                if int(parID) in finishedJobs and (finishedJobs[int(parID)] != "s"):
                    threadCanRun = False

        if threadCanRun and not stopThreads:
            onu_logging.logMessage("Starting job " + str(jobID))
            onu_logging.logMessage(str(datetime.now()))

            timeoutCounter = 3
            jobResult = None
            job_res = None

            #Run job and catch any exception. Retry 3 times and then exit
            #for timeoutCounter in range(1,3):
            while True:
                try:
                    #pprint(self.job)
                    job_res = run_job(self.job["url"])
                    jobResult = json.loads(job_res.text)
                    break
                except requests.exceptions.Timeout as e:
                    if timeoutCounter > 0 :
                        timeoutCounter = timeoutCounter - 1
                        frameinfo = getframeinfo(currentframe())
                        onu_logging.logErrorMessage("Response timed out, retrying... : %s" % e, frameinfo.lineno)
                    else:
                        frameinfo = getframeinfo(currentframe())
                        onu_logging.logErrorMessage("Retry limit, aborting main job", frameinfo.lineno)
                        break
                except (requests.exceptions.ConnectionError, requests.exceptions.RequestException)  as e:    #Handles connection error
                    if timeoutCounter > 0 :
                        timeoutCounter = timeoutCounter - 1
                        frameinfo = getframeinfo(currentframe())
                        onu_logging.logErrorMessage("Connection refused, retrying... : %s" % e, frameinfo.lineno)
                        time.sleep(10)
                        if "403" in str(e):
                            frameinfo = getframeinfo(currentframe())
                            onu_logging.logErrorMessage("Session lost, trying to create a new one..." , frameinfo.lineno)
                            authResp = auth_admin_user()
                    else:
                        onu_logging.logMessage("Retry limit, aborting main job")
                        break
                except Exception as e:
                    frameinfo = getframeinfo(currentframe())
                    onu_logging.logErrorMessage("Could not load response : %s" % e , frameinfo.lineno)
                    onu_logging.logJsonMessage("JOB %s:\n" % jobID, self.job)
                    onu_logging.logJsonMessage("",jobResult)
                    break

            if ( (job_res and jobResult != None) and job_res.status_code == requests.codes.ok):

                #jobResult = json.loads(job_res.text)
                if 'result' in jobResult:
                    onu_logging.logMessage("RESULT :  "+ str(jobID) + ": " + jobResult['result'])
                    if jobResult['result'] == 'e':
                        frameinfo = getframeinfo(currentframe())
                        onu_logging.logErrorMessage(jobResult['msg'], frameinfo.lineno)

                isThreadable = int(self.job["threadable"])
                #If job is threadable we're gonna have an array of ID's in the result to create subJobs
                #Additionally, if we need extra threading (cacheId, etc) the field "threadable" will be in the array as well
                if isThreadable == 1 and not stopThreads:
                    if 'threadable' in jobResult:
                        subJobsThreadable = jobResult['threadable']
                    else:
                        subJobsThreadable = '0'

                    run_multiple_sub_jobs(self.job["url"], jobResult['subJobs'], subJobsThreadable, self.threadLevel)

                #create threads for the jobs dependent on this job
                # parent = -1 means these are sub jobs
                #if parent is > -1:
                #    create_new_threads(self,jobDep)

                if jobID > 0 and not stopThreads:
                    if jobID in depThreadList:
                        onu_logging.logMessage("Job "+ str(jobID) + " was removed from the dependent list")
                        del depThreadList[jobID]

                        if 'result' in jobResult:
                            jobResult = jobResult['result']
                            finishedJobs[jobID] = jobResult
                    dependencies = None

                    if subJobID is None:
                        timeoutCounter = 3
                        #for timeoutCounter in range(1,3):
                        while True:
                            try:
                                dependencies = fetch_dependencies(jobID)
                                break
                            except requests.exceptions.Timeout as e:
                                if timeoutCounter > 0 :
                                    timeoutCounter = timeoutCounter - 1
                                    frameinfo = getframeinfo(currentframe())
                                    onu_logging.logErrorMessage("Response timed out, retrying... : %s" % e, frameinfo.lineno)
                                else:
                                    onu_logging.logMessage("Retry limit, aborting main job")
                                    break
                            except (requests.exceptions.ConnectionError, requests.exceptions.RequestException)  as e:    #Handles connection error
                                if timeoutCounter > 0 :
                                    timeoutCounter = timeoutCounter - 1
                                    frameinfo = getframeinfo(currentframe())
                                    onu_logging.logErrorMessage("Connection refused, retrying... : %s" % e, frameinfo.lineno)
                                    time.sleep(10)
                                    if "403" in str(e):
                                        frameinfo = getframeinfo(currentframe())
                                        onu_logging.logErrorMessage("Session lost, trying to create a new one..." , frameinfo.lineno)
                                        authResp = auth_admin_user()
                                else:
                                    onu_logging.logMessage("Retry limit, aborting main job")
                                    break
                            except Exception as e:
                                frameinfo = getframeinfo(currentframe())
                                onu_logging.logErrorMessage("Could not fetch dependencies : %s" % e, frameinfo.lineno)
                                break

                        if not dependencies is None:
                            create_new_threads(self.threadLevel, self,dependencies)

            else:
                onu_logging.logMessage("Job "+str(jobID) + " failed")
                finishedJobs[jobID] = 'f'

        else:
            onu_logging.logMessage("Child thread cannot run because parent hasn't finished successfully")

        onu_logging.logMessage("Exiting job " + str(jobID))

        globalConnCounter = globalConnCounter - 1

        onu_logging.logMessage("Number of global threads: {0}".format(globalConnCounter))


        self.timeEnd = datetime.now()
        tdelta = self.timeEnd - self.timeStart
        seconds = tdelta.total_seconds()
        #if isinstance(jobID, int):
            #print "Job "+ str(jobID) + " took "+ datetime.fromtimestamp(int(seconds)).strftime('%H:%M:%S')
        #else:
        message = "Job "+ str(jobID) + " took "+ str(seconds) +"\n"
        message += "ended at: " + str(datetime.now())
        onu_logging.logMessage(message)

###################START run_job########################
#Run the cron job by calling the url
def run_job(url):

    fullURL = serverBaseURL + str(url)
    result = session.get(fullURL, stream=False, verify = False, timeout=1800)
    #session.close()
    result.raise_for_status()

    return result
###################END run_job########################


###################START run_multiple_sub_jobs########################
def run_multiple_sub_jobs(url,subJobs,threadable,threadLevel):

    subThreads = []

    global globalConnCounter
    subJobs = json.loads(subJobs)

    if isinstance(subJobs,(list)):
        subJobsIter = enumerate(subJobs)
    elif isinstance(subJobs,(dict)):
        subJobsIter = subJobs.iteritems()

    subJobsThreadCounter = 0
    for key, firstSubId in subJobsIter:
        #firstSubId should never be empty
        if firstSubId:
            #if is array fetch firstSubId and secondSubId id
            if not isinstance(subJobs[key], (basestring,int)):
                for secondSubId in subJobs[key]:
                    #Need this kind of loop because the threads immediately take the open slots and we don't wanna lose any job in this gap
                    while True:
                        if globalConnCounter < globalConn or threadLevel > 1:
                            job = {"url":url+"/"+key+"/"+secondSubId, "threadable":threadable}
                            thread = jobThread(threadLevel + 1, -1, job,-1, url+"/"+key+"/"+secondSubId)
                            thread.daemon = True
                            globalConnCounter = globalConnCounter + 1
                            thread.start()
                            #In order to limit the lower threading level to 1 we wait for the created thread to finish with join()
                            if threadLevel > 1:
                                thread.join()
                            else:   #if not we just add to the subThreads list and wait later
                                subThreads.append(thread)
                            break
                        else :
                            #print "NO CONNECTION SLOT: "+str(firstSubId)+"/"+str(secondSubId)
                            pass
            else:
                while True:
                    if globalConnCounter < globalConn or threadLevel > 1:
                        job = {"url":url+"/"+firstSubId, "threadable":threadable}
                        thread = jobThread(threadLevel + 1, -1, job,-1, url+"/"+firstSubId)
                        thread.daemon = True
                        globalConnCounter = globalConnCounter + 1
                        thread.start()
                        #for the first level of subThreads
                        if threadLevel < 4:
                            subThreads.append(thread)
                        if threadLevel > 3:
                            #probably a bad way to handle this but:
                            #if we have more than 1 subjobs at level 2, by getting here we started the 3rd one so we wait.
                            if subJobsThreadCounter > 1:
                                thread.join()
                                subJobsThreadCounter -= 1
                            else:
                                subJobsThreadCounter += 1
                                subThreads.append(thread)
                        break
                    else :
                        #print "NO CONNECTION SLOT: "+str(firstSubId)+"/"+str(secondSubId)
                        pass
    for t in subThreads:
        t.join()
###################END run_multiple_sub_jobs########################


###################START create_new_threads########################
#Creates new threads from the list of jobs
def create_new_threads(threadLevel,parent,jobs):

    global globalConnCounter
    for (jobID, job) in jobs.items():

        jobID = int(jobID)
        thread = jobThread(threadLevel, parent, job, jobID)
        thread.daemon = True
        if int(job["enabled"]) == 1:
            #In order to overload the server with requests we limit the number of active threads
            while globalConnCounter > globalConn - 1:
                #print "Job "+ str(jobID) + " waiting for connection slot!"
                pass

            #For multiple dependencies, check if new job wasn't or isn't being processed already
            if jobID not in finishedJobs and jobID not in depThreadList:
                if not globalConnCounter is globalConn:

                    depThreadList[jobID] = thread
                    globalConnCounter = globalConnCounter + 1
                    thread.start()

###################END create_new_threads########################



###################START fetch_dependencies########################
#Fetch the dependencies of job: jobID
def fetch_dependencies(jobID):

    onu_logging.logMessage("Fetching jobs dependent on job " + str(jobID))

    if jobID is -1 :
        jobID = ""

    #Create the full url to be called:
    #   - /dependency retrieves all the jobs with no dependencies
    #   - /depency/jobID/ gives the jobs which depend on jobID
    fullURL = serverBaseURL + "/dependency/" + str(jobID)

    #Get the dependencies through the session
    #Raise exceptions
    depResponse = session.get(fullURL, verify = False, timeout=1800)
    depResponse.raise_for_status()

    #First convert the HTML response text to a clean JSON object
    dependencies = json.loads(depResponse.text)

    message = "Job " + str(jobID) + " dependencies: "
    onu_logging.logJsonMessage(message, dependencies)

    return dependencies
###################END fetch_dependencies########################

###################START check_multiple_dependencies########################
#Check if a job depends on multiple other jobs and returns the list
def check_multiple_dependencies(jobID):

    fullURL = serverBaseURL + "/dependee/" + str(jobID)
    result = session.get(fullURL, verify = False, timeout=1800)
    result.raise_for_status()

    parArray = json.loads(result.text)

    return parArray
###################END check_multiple_dependencies########################

###################END fetch single job########################
def fetch_job(jobID):

    fullURL = serverBaseURL + "/job/" + str(jobID)
    result = session.get(fullURL, verify = False, timeout=1800)
    result.raise_for_status()

    parArray = json.loads(result.text)

    return parArray
###################END check_multiple_dependencies########################

###################START clean_logs########################
def clean_logs():
    fullURL = serverBaseURL + "/clean-logs"
    result = session.get(fullURL, verify = False, timeout=1800)
    result.raise_for_status()

    return result

###################START clean_logs########################

###################START auth_admin_user########################
#Authenticates admin user session to access overnight updates urls
def auth_admin_user():

    global adminUser, adminPwd
    fullUrl = serverBaseURL + '/user/login'      #Pass the appropriate data to the user/login form
    loginData= {
        'name': adminUser,
        'pass': adminPwd,
        'form_id': "user_login",
    }
    print(fullUrl)
    #Post the data to the form and save the response.
    #Raise exceptions
    loginResponse = session.post(fullUrl, data=loginData, verify = False, timeout=1800)
    #loginResponse.raise_for_status()

    return loginResponse

###################END auth_admin_user########################

###################START set_base_url########################
#Authenticates admin user session to access overnight updates urls
def set_base_url(environment):

    global adminPwd

    baseURL = str(environment)
    adminPwd = ""

    if environment == "prod":
        baseURL = ""
        adminPwd = ""
    if environment == "dev":
        baseURL = ""
        adminPwd = ""

    return baseURL
###################END set_base_url########################

def stopProcesses():
    global stopThreads
    stopThreads = True
    while finished == False:
        pass


stopThreads = False
finished = False

serverBaseURL = ""
adminUser = ""
adminPwd = ""
globalConnCounter = 0
globalConn = 13      #Limits the number of overall connections
session = requests.session()    #Session object used for all requests

#These two lists are used to manage multiple dependencies, they wouldn't be necessary if we didn't have them
depThreadList = {}              #
finishedJobs = {}

bufferLog = ""

#To run: overnightupdates environmentName jobId
# No jobId runs all
#if __name__ == "__main__":
def startOnu(environment, jobID):
    global bufferLog, serverBaseURL, finished
    timeStart = datetime.now()

    serverBaseURL = set_base_url(environment)

    #Handle critical connection errors: 4xx, 5xx
    #Failed auth is verified manually in the response
    try:
        authResp = auth_admin_user()
    except Exception as e:   #Takes care of all the other exceptions
        frameinfo = getframeinfo(currentframe())
        onu_logging.logErrorMessage("Admin authentication: failed, aborting Main thread! : %s" % e, frameinfo.lineno)
        exit()

    #Using this method to search the response text and find if the login was succesfull
    #Used inwith a Drupal website, can be altered to fit any other
    soup = BeautifulSoup(authResp.text)                 #Fetch the response page HTML
    loginFailed = soup.find("body", "not-logged-in")    #Login failed if the page has the 'not-logged-in' class

    #Continue the process if the user is logged in
    if not loginFailed:
        onu_logging.logMessage("Admin authentication: success")
        try:
            clean_logs()            #Clean watchdog logs related to overnight updates
            onu_logging.logMessage("Cleaned old logs")
        except Exception as e:
            frameinfo = getframeinfo(currentframe())
            onu_logging.logErrorMessage("Couldn't clean old logs...continuing anyway : %s" % e, frameinfo.lineno)

        dependencies = []
        timeoutCounter = 3

        if jobID is -1:
            while True:
                try:
                    dependencies = fetch_dependencies(jobID)
                    break
                except requests.exceptions.Timeout as e:
                    if timeoutCounter > 0 :
                        timeoutCounter = timeoutCounter - 1
                        frameinfo = getframeinfo(currentframe())
                        onu_logging.logErrorMessage("Response timed out, retrying : %s" % e, frameinfo.lineno)
                    else:
                        onu_logging.logMessage("Retry limit, aborting main job")
                        break
                except (requests.exceptions.ConnectionError, requests.exceptions.RequestException)  as e:    #Handles connection error
                    if timeoutCounter > 0 :
                        timeoutCounter = timeoutCounter - 1
                        frameinfo = getframeinfo(currentframe())
                        onu_logging.logErrorMessage("Connection refused, retrying : %s" % e, frameinfo.lineno)
                        time.sleep(10)
                        if "403" in str(e):
                            onu_logging.logMessage("Session lost, trying to create a new one...")
                            authResp = auth_admin_user()
                    else:
                        onu_logging.logMessage("Retry limit, aborting main job")
                        break
                except Exception as e: #Takes care of all the other exceptions
                    frameinfo = getframeinfo(currentframe())
                    onu_logging.logErrorMessage("Could not load response : %s" % e, frameinfo.lineno)
                    break
        else:
            try:
                job = fetch_job(jobID)
                if int(job["enabled"]) == 1:
                    thread = jobThread(1,0, job, jobID)
                    thread.daemon = True
                    depThreadList[jobID] = thread
                    globalConnCounter = globalConnCounter + 1
                    thread.start()

            except Exception as e:
                frameinfo = getframeinfo(currentframe())
                onu_logging.logErrorMessage("Couldn't fetch job : %s" % e, frameinfo.lineno)

        if len(dependencies) > 0 and jobID is -1:
            onu_logging.logMessage("Creating main jobs threads.\n")
            create_new_threads(1, 0,dependencies)

        #Do not terminate program until the only active thread is Main
        while threading.active_count() > 1:
            terminate = True
            for thread in threading.enumerate():
                if(thread.__class__.__name__ == 'jobThread'):
                    terminate = False
            if terminate:
                finished = True
                break
            pass

    else:
        onu_logging.logMessage("Admin authentication failed, aborting execution!")

    timeEnd = datetime.now()
    tdelta = timeEnd - timeStart
    seconds = tdelta.total_seconds()
    #print "Main thread took "+ datetime.fromtimestamp(int(seconds)).strftime('%H:%M:%S')
    onu_logging.logMessage("Main thread ended at " + str(datetime.now()))

    #print "Job results overview: \n"
    #pprint(finishedJobs)

    onu_logging.logMessage("Main thread finished...exiting")


if __name__ == "__main__":
    args = sys.argv
    if len(args) > 1:
        environment = str(args[1])
    else:
        print "Environment name not entered, exiting..."
        exit()

    jobID = -1               #If no arguments are passed we fetch all the jobs with no dependencies
    if len(args) > 2:
        jobID = int(args[2])       #If script is called with arguments means we're calling a specific job

    startOnu(environment, jobID)