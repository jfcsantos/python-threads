from flask import Flask, render_template, request
from var_dump import var_dump
import thread
import onu, onu_logging
app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route("/onu/")
def onuStart():
    if request.method == 'GET':
        #return request.args['env']
        jobId = -1               #If no arguments are passed we fetch all the jobs with no dependencies
        if 'jobId' in request.args:                           
            jobId = request.args['jobId']       #If script is called with arguments means we're calling a specific job

        onuThread = thread.start_new_thread(onu.startOnu,(request.args['env'], jobId))

        return render_template('onu.html')

@app.route("/onu/log")
def onuLog():
    return render_template('onu.html', log=onu_logging.returnLog())

@app.route("/onu/stopONU")
def stopONU():
    onu.stopProcesses()
    return render_template('end.html', log=onu_logging.returnLog())

if __name__ == "__main__":
    app.run(debug=True)