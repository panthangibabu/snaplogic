#! /usr/bin/env python
# SnapLogic - Data Integration
#
# Copyright (C) 2015, SnapLogic, Inc.  All rights reserved.
#
# This program is licensed under the terms of
# the SnapLogic Commercial Subscription agreement.
#
# "SnapLogic" is a trademark of SnapLogic, Inc.

import argparse
import datetime
import sys
import time
import json
import base64
import urllib2
import os
import iso8601


DEFAULT_USER = "" # Replace with valid username or pass it from jenkins as a parameter
DEFAULT_PW   = "" #this passwd is not a valid one, either give a valid passwd or pass it from jenkins
DEFAULT_ORG  = ''

#Plexes defing
   #Standard plex
DEFAULT_PLEXNAME = ''
   #SPark plex
DEFAULT_SPARK_PLEXNAME = ''

DEFAULT_PS = '' #project Space
DEFAULT_PROJECT ='' #Project
DEFAULT_POD_URI = '' #POD
DEFAULT_MAX_PIPELINE_EXECUTION_TIME = 300    # in seconds
DEFAULT_REST_TIMEOUT = 120 # timeout on control plane requests

#Filter
DEFAULT_FILTER='' #Filter given Multiple strings
DEFAULT_ONLY= 'ALL'
#global variables
server_uri=""
ns={}
plexList={}
pipelinesList={}
constructList={}


start_date = datetime.datetime.utcnow()

def main():
    global server_uri, ns,pipelinesList,constructList
    parser = argparse.ArgumentParser()

    parser.add_argument('--thread-count', help=('The number of threads to be started. Each '
                                                'thread will sequentially execute its pipelines'),
                        default=1, type=int)

    parser.add_argument('--pod-uri', type=str, help='The hostname of the pod you are running on. '
                                                    'Must match the prefix of the account files',
                        default=DEFAULT_POD_URI)
    parser.add_argument('--projectspace', type=str, help='ProjectSpace where project resides', default=DEFAULT_PS)
    parser.add_argument('--project', default=DEFAULT_PROJECT, type=str, help='Project to execute')
    parser.add_argument('--plex', type=str,
                        help='Plex name as displayed in plex dropdown in designer or from dashboard '
                             'EX: DevCluster',
                        default=DEFAULT_PLEXNAME)
    parser.add_argument('--spark-plex', type=str,
                        help='Plex name as displayed in plex dropdown in designer or from dashboard '
                             'EX: DevCluster',
                        default=DEFAULT_SPARK_PLEXNAME)

    parser.add_argument('--username', type=str, help='Username to run pipelines as',
                        default=DEFAULT_USER)

    parser.add_argument('--password', type=str, help='Password to use', default=DEFAULT_PW)

    parser.add_argument('--org', type=str, help='Org to use', default=DEFAULT_ORG)

    parser.add_argument('--outfile', type=str, help='Result file', default='<std>')

    parser.add_argument('--timeout', type=str, help='Pipeline execution timeout',
                        default=DEFAULT_MAX_PIPELINE_EXECUTION_TIME)

    parser.add_argument('--exetype', type=str, help='Execution type Of mode pipelines', default=DEFAULT_ONLY)
    parser.add_argument('--filter', type=str, help='Filter expression', default=DEFAULT_FILTER)

    ns = parser.parse_args()
    #print ns

    # Here Check the spark plex is empty or not , If empty put both use same plex
    if not bool(ns.spark_plex) and not bool(ns.plex):
        print "Provide any plex name to run pipelines"
    if not bool(ns.spark_plex):
        ns.spark_plex = ns.plex
    elif not bool(ns.plex):
        ns.plex = ns.spark_plex

    filter_list=[]
    ns.filter_list=ns.filter.split(",")
    print ns
    # sys.exit()

    server_uri = 'https://' + ns.pod_uri

    # org_id of selected org
    org_id=getOrgId()
    ns.org_id=org_id
    print "**********************************"
    print 'Org : %s \t AND Org_id : %s\n' % (ns.org,ns.org_id)
    #print "**********************************"

    # ----------------------------------------------create files and result ------------------------
    # add the directory ns it self
    ns.dirname = "%s/%d.%d.%d/%s" % ("Jenkins Result", start_date.year, start_date.month, start_date.day, ns.project)
    # print ns.dirname

    # ------------------------------------------------standard file-------------------
    ns.standardFile = ""
    # ------------------------------------------------spark file-------------------
    ns.sparkFile = ""
    # ----------------------------------------------MR File---------------------------
    ns.MRFile = ""
    # ----------------------------------------------create files and exit----------------------------


    # --------------------------------------plexes----------------------
    ns.plex_rid = getPlexRID(ns.plex)
    ns.spark_plex_rid = getPlexRID(ns.spark_plex)

    #print "**********************************\n"
    print 'Standard plex : %s \t AND plex_rid : %s ' %(ns.plex,ns.plex_rid)
    print 'Spark plex : %s \t AND Spark plex_rid : %s ' % (ns.spark_plex, ns.spark_plex_rid)
    #print "**********************************\n"
    #print ns
    #sys.exit()
    # --------------------------------------------------------plex end---------------



    # ----------------------------------Pipelines sarted-----------------------------------
    pipelinesList = getAllPipelines()

    #print "**********************************\n"
    #print 'pipelinesList : '
    #print pipelinesList
    #print "**********************************\n"

    #Construct the List For saparate All Modes ex: Standard,Spark and Map Reduce Mode
    constructList=pipelineListConstruct()
    #print constructList
    #matching = [s for s in constructList["Standard"] if "hive".lower() in s['name'].lower()]
    #print matching

    if constructList["Standard"]:
      print "\n-----------------*-----------------:: Standard Pipelines Start ::-----------------*-----------------\n"
      # print_constructList(constructList["Standard"])
      # print "\n-----------------*-----------------*-----------------*-----------------*----------------- \n"
      exe_Standard_Pipelines=run_StandardPipelines()
      exe_Standard_Pipelines.sort(key=lambda x: x['status'])
      print_Result(exe_Standard_Pipelines)
      print "\n-----------------*-----------------:: Standard Pipelines END::-----------------*-----------------\n"
    if constructList["Spark"]:
      print "\n-----------------*-----------------:: Spark Pipelines Start ::-----------------*-----------------\n"
      # print_constructList(constructList["Spark"])
      # print "\n-----------------*-----------------*-----------------*-----------------*-----------------\n"
      exe_Spark_Pipelines=run_sparkPipelines()
      exe_Spark_Pipelines.sort(key=lambda x: x['status'])
      print_Result(exe_Spark_Pipelines)
      print "\n-----------------*-----------------:: Spark Pipelines END ::-----------------*-----------------\n"
    #if constructList["Mapreduce"]:
      #print "\n-----------------*-----------------:: Map Reduce Pipleines Start ::-----------------*-----------------\n"
      #print_constructList(constructList["Mapreduce"])
      #print "\n---------------------------------- \n"
      #exe_Mapreduce_Pipelines=run_MapreducePipelines()
      #exe_Mapreduce_Pipelines.sort(key=lambda x: x['status'])
      #print_Result(exe_Mapreduce_Pipelines)
      # print "\n-----------------*-----------------:: Map Reduce Pipleines END ::-----------------*-----------------\n"

    # ----------------------------------Pipelines ended-----------------------------------


    print 'Executing pipelines with \n\t user as %s \n\t timeout as %s \n\t plex as %s \n\t Hadoop plex as %s \n \t' % (
        ns.username, ns.timeout, ns.plex, ns.spark_plex)


#Here User Defined Methods
def get(uri, username, password, raw=False):

    # Add the username and password.
    base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
    request = urllib2.Request(uri)
    request.add_header("Authorization", "Basic %s" % base64string)
    request.add_header("x-snapi-accept", "application/json")
    request.add_header('Content-Type', 'application/json')
    request.add_header('Accept', 'application/json')
    try:
        response = urllib2.urlopen(request, timeout=120)
    except urllib2.HTTPError, e:
        print str(e);
        return {}
    except urllib2.URLError, e:
        print str(e);
        return {}
    info = response.info()

    if not raw and 'content-type' in info and 'application/json' == info['content-type']:
        out = json.loads(response.read())
    else:
        out = response.read()
    if 'response_map' in out:
        return out['response_map']
    return out


def post(uri, data, username, password, rest_timeout=DEFAULT_REST_TIMEOUT):
    # Add the username and password.
    base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
    request = urllib2.Request(uri)
    request.add_header("Authorization", "Basic %s" % base64string)
    request.add_header('Content-Type', 'application/json')
    request.add_header('Accept', 'application/json')
    out = ''
    try:
        response = urllib2.urlopen(request, json.dumps(data), timeout=rest_timeout)
        msg = response.read();
        info = response.info();
        out = json.loads(msg)
    except urllib2.URLError, e:
        out = str(e);
    except urllib2.HTTPError, e:
        out = str(e);
        # "%s %s" % (e.code, e.reason)
    except Exception, e:
        out = str(e)
    if 'response_map' in out:
        return out['response_map']

    return out


# newly added def ----------------------------------------------------------------------------------------

def getAllPipelines():
    global server_uri, ns
    pipelines_rest_api=server_uri + '/api/1/rest/asset/list/' + ns.org + '/'+ ns.projectspace +'/' +ns.project + '?asset_type=Pipeline'
    pipelines_rest_api=replaceString(pipelines_rest_api)
    #print "**********************************\n"
    print 'pipelines_rest_api : %s\n' % (pipelines_rest_api)
    print "**********************************\n"
    pipelineList = get(pipelines_rest_api, ns.username, ns.password)

    if pipelineList:
       print '\n list of pipelines api call is successful \n'
       #print pipelineList
    else:
        print '\n Pipelines are does not exists :: Terminated the Program.\n'
        sys.exit();
    return pipelineList

def replaceString(string):
    # replace the space with '%20' in string
    string = string.replace(" ", "%20")
    return string


# get the all Orgs for given user
def getAllOrgs():
    global server_uri, ns
    org_rest_api = server_uri + '/api/1/rest/asset/user/' + ns.username
    org_rest_api = replaceString(org_rest_api)
    print "\n org_rest_api ==>" + org_rest_api
    user = get(org_rest_api, ns.username, ns.password)
    orglist = user.get("org_snodes")
    return orglist


def getOrgId():
    orglist = getAllOrgs()
    # print orglist
    for key in orglist:
        orgname = orglist.get(key).get("name")
        if orgname == ns.org:
            org_id = key
    return org_id


# get the all Orgs for given user
def getAllPlexes():
    global server_uri, ns
    plex_rest_api = server_uri + '/api/1/rest/plex/org/' + ns.org
    plex_rest_api = replaceString(plex_rest_api)
    print "\n org_rest_api ==>" + plex_rest_api
    plexList = {}
    plexs = get(plex_rest_api, ns.username, ns.password)
    # print plexList
    for plex in plexs:
        # print plex["label"] + " = " + plex["path"] + " = " + plex["runtime_path_id"] + "\n"
        plexList[plex["label"]] = plex["runtime_path_id"]
    return plexList


def getPlexRID(plexname):
    global plexList
    if not bool(plexList):
        plexList = getAllPlexes()

    # print plexList
    plexRId = ""
    for key in plexList:
        if key == plexname:
            plexRId = plexList.get(key)
    # print 'plexRId : %s' % plexRId
    return plexRId

def pipelineListConstruct():
    global pipelinesList,ns
    #print pipelinesList
    constructList={}
    Standard= []
    Spark = []
    Mapreduce = []
    for pipe in pipelinesList['entries']:
        if ns.filter_list and not any(x.lower() in pipe['name'].lower() for x in ns.filter_list):
            # ignore
            continue;

        if any('[x]' in pipe['name'].lower() for x in ns.filter_list):
            print pipe['name']
            continue;

        list = {}
        list["name"]=pipe["name"]
        list["snode_id"] =pipe["snode_id"]

        if 'target_runtime' in pipe:
            if (ns.exetype and (ns.exetype.upper()=="ALL" or ns.exetype.upper()=="SPARK")) and pipe['target_runtime'] == "spark":
                list["target_runtime"] = pipe["target_runtime"]
                Spark.append(list)
            elif (ns.exetype and (ns.exetype.upper()=="ALL" or ns.exetype.upper()=="MAP REDUCE")) and pipe['target_runtime'] == "map-reduce":
                list["target_runtime"] = pipe["target_runtime"]
                Mapreduce.append(list)
        elif ns.exetype and (ns.exetype.upper()=="ALL" or ns.exetype.upper()=="STANDARD"):
            list["target_runtime"] = "Standard"
            Standard.append(list)

    constructList["Standard"]=Standard
    constructList["Spark"] = Spark
    constructList["Mapreduce"] = Mapreduce

    return constructList

def print_constructList(list):
    sys.stdout.write("\n")
    sys.stdout.write("%-60s" % 'Name'[:49])
    sys.stdout.write("%-10s" % 'Mode')
    sys.stdout.write("%-10s" % 'Snode id')
    sys.stdout.write("\n\n")
    for mapdict in list:
        sys.stdout.write("%-60s  " % mapdict['name'][:49])
        sys.stdout.write("%-10s  " % mapdict['target_runtime'])
        sys.stdout.write("%-10s  " % mapdict['snode_id'])
        sys.stdout.write("\n")

def print_Result(list):
    global ns
    sys.stdout.write("\n")
    sys.stdout.write("%-60s" % 'Name'[:49])
    sys.stdout.write("%-10s" % 'Mode')
    sys.stdout.write("%-10s" % 'Status')
    sys.stdout.write("%-10s" % 'Completed Time')
    sys.stdout.write("\n\n")

    if ns.filter_list:
        for filter in ns.filter_list:
            subList=[s for s in list if filter.lower() in s['name'].lower()]
            success = 0
            failed = 0
            prepare_failed = 0
            timed_out = 0
            ex_total_time = 0.0
            num_pipelines = 0
            print "\n-----------------*-----------------:: "+filter+" Pipelines Started ::-----------------*-----------------\n"
            #print subList;
            for mapdict in subList:
                # increments the Status by state
                if mapdict['status'] and mapdict['status'] == "Completed":
                    success += 1
                elif mapdict['status'] and mapdict['status'] == "Failed" or mapdict['status'] == "Stopped":
                    failed += 1
                elif mapdict['status'] and mapdict['status'] == "Prepare failed":
                    prepare_failed += 1
                elif mapdict['status'] and mapdict['status'] == "TIMED OUT":
                    timed_out += 1

                #count the Total Time Duration
                ex_total_time+=mapdict['completed time']
                num_pipelines+=1


                sys.stdout.write("%-60s  " % mapdict['name'][:49])
                sys.stdout.write("%-10s  " % mapdict['target_runtime'])
                sys.stdout.write("%-10s  " % mapdict['status'])
                sys.stdout.write("%-10s  " % mapdict['completed time'])
                sys.stdout.write("\n")
            print "\n-----------------:: " + filter + " Pipelines :: \n"
            print "ExecutionSuccess %d  ExecutionFailed %d  PrepareFailedDuringExecution %d timed_out %d" % (success,failed,prepare_failed,timed_out)
            print "total execution time %.3f" %(ex_total_time)
            print "total"+filter+"Pipelines %d" %(num_pipelines)
            print "\n-----------------*-----------------:: " + filter + " Pipelines End ::-----------------*-----------------\n"
    else :
        print "\n-----------------*-----------------:: Pipelines Started ::-----------------*-----------------\n"
        success = 0
        failed = 0
        prepare_failed = 0
        timed_out= 0
        ex_total_time=0
        num_pipelines= 0
        for mapdict in list:

            # increments the Status by state
            if mapdict['status'] and mapdict['status'] == "Completed":
                success+= 1
            elif mapdict['status'] and mapdict['status'] == "Failed" or mapdict['status'] == "Stopped":
                failed+= 1
            elif mapdict['status'] and mapdict['status'] == "Prepare failed":
                prepare_failed+= 1
            elif mapdict['status'] and mapdict['status'] == "TIMED OUT":
                timed_out+= 1

            # count the Total Time Duration
            ex_total_time += mapdict['completed time']
            num_pipelines += 1

            sys.stdout.write("%-60s  " % mapdict['name'][:49])
            sys.stdout.write("%-10s  " % mapdict['target_runtime'])
            sys.stdout.write("%-10s  " % mapdict['snode_id'])
            sys.stdout.write("%-10s  " % mapdict['status'])
            sys.stdout.write("%-10s  " % mapdict['completed time'])
            sys.stdout.write("\n")
        print "\n-----------------:: Pipelines :: \n"
        print "ExecutionSuccess %d  ExecutionFailed %d  PrepareFailedDuringExecution %d" % (success, failed, prepare_failed)
        print "total execution time %.3f" % (ex_total_time)
        print "total Pipelines %d" % (num_pipelines)
        print "\n-----------------*-----------------:: Pipelines End ::-----------------*-----------------\n"


#Run Spark Pipelines
def run_sparkPipelines():
    global ns,constructList,server_uri
    if constructList["Spark"]:
        sparkPipelines=constructList["Spark"]
        success = 0
        failed = 0
        prepare_failed = 0
        total=0
        executed_pipelines=[]
        # Spark file created
        if ns.sparkFile == "":
            outfileSpark = createOutFile("spark")
            ns.sparkFile = outfileSpark;
            if ns.sparkFile:
                try:
                    fp = open(ns.dirname + "/" + ns.sparkFile, 'w+')
                    print 'Spark Mode Output sent to ==> %s\n' % ns.sparkFile
                    fp.write('Pipeline Name,Status,StageName,StageStatus\n')
                except Exception, e:
                    pass;
        for pipe in sparkPipelines:
            if 'target_runtime' in pipe and pipe['target_runtime'] == "spark":
                if ns.filter_list and not any(x.lower() in pipe['name'].lower() for x in ns.filter_list):
                    # ignore
                    continue;

                if any('[x]' in pipe['name'].lower() for x in ns.filter_list):
                    print pipe['name']
                    continue;
                sys.stdout.write("%-60s  " % pipe['name'])
                prepare_url = server_uri + '/api/1/rest/pipeline/prepare/' + pipe['snode_id'] + '?target_runtime=spark'
                prepare_url = replaceString(prepare_url)
                #print 'prepare_url :%s' % prepare_url
                prepared = post(prepare_url,
                                {'runtime_path_id': ns.spark_plex_rid, "runtime_label": ns.spark_plex, "do_start": True,
                                 "async": True, "priority": 10, "queueable": True}, ns.username, ns.password)
                #print 'prepared :' % prepared
                #print prepared

                if (prepared and 'runtime_id' in prepared):
                    #print 'org id is %s and runtime id is %s' % (ns.org_id, prepared['runtime_id'])
                    sys.stdout.write("Prepared, Start Executing ");
                    i = 0;
                    t0 = time.time();
                    ex_status=""
                    ex_time =0
                    total_ex_time=0
                    while 1:
                        time.sleep(2)
                        try:
                            status_url=server_uri + '/api/2/' + ns.org_id + '/rest/pm/runtime/' +prepared['runtime_id'] + '?level=detail'
                            status_url=replaceString(status_url)
                            status = get(status_url,ns.username, ns.password)
                            if 'state' in status and status['state'] == 'Failed' and status.get("spark_metrics").get(
                                    'error_msg') != None:
                                errormsg = 'Failed: error msg is :- ' + status.get("spark_metrics").get('error_msg')
                                if fp:
                                    fp.write("\"%s\",%s,%s,%s\n" % (pipe['name'],
                                                                    errormsg, '', ''))
                                    fp.flush()
                            elif 'state' in status and (
                                        status['state'] == 'Failed' or status['state'] == 'Stopped' or status[
                                'state'] == 'Completed'):
                                job_spark_metrics = status.get("spark_metrics").get("jobs")
                                for job in job_spark_metrics:
                                    stages = job_spark_metrics.get(job).get('stages')
                                    stagestatus = job_spark_metrics.get(job).get('status')
                                    for stage in stages:
                                        name = stages.get(stage).get('name')
                                        if fp:
                                            fp.write("\"%s\",%s,%s,%s\n" % (pipe['name'],
                                                                            status['state'], name, stagestatus))
                                            fp.flush()
                            if 'state' in status and (status['state'] == 'Failed' or status['state'] == 'Stopped'):
                                print '   [' + status['state'] + ']'
                                failed += 1
                                ex_status=status['state']
                                ex_time = time.time() - t0
                                break

                            elif 'state' in status and status['state'] == 'Completed':
                                print '   [' + status['state'] + ']'
                                success += 1
                                ex_status = status['state']
                                ex_time=time.time() - t0
                                break
                            else:
                                i += 1
                                if i > 3:
                                    if i%3==0:
                                      sys.stdout.write('.')

                                if (time.time() - t0) > int(ns.timeout):
                                    print ' [TIMED OUT]'
                                    failed += 1
                                    ex_status = "TIMED OUT"
                                    ex_time = time.time() - t0

                                    # Here is stop the pipeline when time out
                                    stop_url = server_uri + '/api/1/rest/pipeline/stop/' + prepared['runtime_id']
                                    stop_url = replaceString(stop_url)
                                    # print stop_url
                                    stop_resp = post(stop_url, {}, ns.username, ns.password)

                                    if fp:
                                        fp.write("\"%s\",%s,%s,%s\n" % (pipe['name'],
                                                                        'TimedOut', '', ''))
                                        fp.flush()
                                    break

                        except Exception, e:
                            print str(e);
                            break;

                else:
                    if 'error_map' in prepared:
                        sys.stdout.write("[Prepare failed] ")
                        if 'snap' in prepared['error_map']:
                            for snap in prepared['error_map']['snap']:

                                if 'error_msg' in prepared['error_map']['snap'][snap]:
                                    sys.stdout.write(
                                        json.dumps(prepared['error_map']['snap'][snap]['error_msg']) +
                                        "  ")
                                else:
                                    sys.stdout.write(json.dumps(prepared['error_map']['snap'][snap]))

                            sys.stdout.write("\n")
                        else:
                            try:
                                sys.stdout.write(json.dumps(prepared['error_map']['error_msg']));
                            except Exception, e:
                                sys.stdout.write(json.dumps(prepared['error_map']));
                    else:
                        print "[Prepare failed] %s" % json.dumps(prepared)
                    if fp:
                        fp.write("\"%s\",%s,%s,%s\n" % (pipe['name'], 'Prepare Failed', '', ''))
                    prepare_failed += 1
                    ex_status = "Prepare failed"
                    ex_time = time.time() - t0
            pipe["status"]=ex_status
            pipe["completed time"] = ex_time
            total_ex_time+=ex_time
            executed_pipelines.append(pipe)
        print "======================================================"
        print "ExecutionSuccess %d  ExecutionFailed %d  PrepareFailedDuringExecution %d total execution time %.3f" % (
        success, failed, prepare_failed,total_ex_time)
    else:
        print "Theier is no Spark Pipelines"

    return executed_pipelines

#Run Standard Pipelines
def run_StandardPipelines():
    global ns,constructList,server_uri
    if constructList["Standard"]:
        Pipelines=constructList["Standard"]
        success = 0
        failed = 0
        prepare_failed = 0
        total=0
        executed_pipelines=[]
        # Standard file created
        if ns.standardFile == "":
            outfileStandard = createOutFile("Standard")
            ns.standardFile = outfileStandard;
            if ns.standardFile:
                try:
                    fp = open(ns.dirname + "/" + ns.standardFile, 'w+')
                    print 'standard Output sent to ==> %s\n' % ns.standardFile
                    fp.write('Pipeline Name,Status,StartTime,Duration,Documents\n')
                except Exception, e:
                    pass;

        for pipe in Pipelines:
            if 'target_runtime' in pipe and pipe['target_runtime'] == "Standard":

                if ns.filter_list and not any(x.lower() in pipe['name'].lower() for x in ns.filter_list):
                    # ignore
                    continue;

                if any('[x]' in pipe['name'].lower() for x in ns.filter_list):
                    print pipe['name']
                    continue;

                # prepare pipeline
                sys.stdout.write("%-60s  " % pipe['name'])
                prepare_url = server_uri + '/api/1/rest/pipeline/prepare/' + pipe['snode_id']
                prepare_url = replaceString(prepare_url)
                prepared = post(prepare_url,{'runtime_path_id': ns.plex_rid}, ns.username, ns.password)

                if (prepared and 'runtime_id' in prepared):
                    t0 = time.time()
                    sys.stdout.write("Prepared ")
                    start_url=server_uri + '/api/1/rest/pipeline/start/' +prepared['runtime_id']
                    start_url = replaceString(start_url)
                    result = post(start_url,{}, ns.username, ns.password)
                    # Todo: Make sure it starts
                    sys.stdout.write("Running ");
                    i = 0;
                    t0 = time.time();
                    ex_status = ""
                    ex_time = 0
                    total_ex_time = 0
                    while 1:
                        time.sleep(2)
                        try:
                            status_url = server_uri + '/api/2/' + ns.org_id + '/rest/pm/runtime/' + prepared[
                                'runtime_id'] + '?level=detail'
                            status_url = replaceString(status_url)
                            status = get(status_url,ns.username, ns.password)
                            if 'state' in status and (status['state'] == 'Failed' or status['state'] == 'Stopped'):
                                print '   [' + status['state'] + ']'
                                failed += 1
                                ex_status = status['state']
                                ex_time = time.time() - t0
                                if fp:
                                    t0 = iso8601.parse_date(status['create_time']);
                                    t1 = iso8601.parse_date(status['time_stamp']);
                                    fp.write("\"%s\",%s,%s,%d,%d\n" % (pipe['name'],
                                                                       status['state'],
                                                                       status['time_stamp'],
                                                                       (t1 - t0).total_seconds(),
                                                                       count_docs(status['snap_map'])))
                                    fp.flush()
                                break

                            elif 'state' in status and status['state'] == 'Completed':
                                print '   [' + status['state'] + ']'
                                success += 1
                                ex_status =status['state']
                                ex_time=time.time() - t0
                                if fp:
                                    t0 = iso8601.parse_date(status['create_time']);
                                    t1 = iso8601.parse_date(status['time_stamp']);
                                    fp.write("\"%s\",%s,%s,%d,%d\n" % (pipe['name'],
                                                                       status['state'],
                                                                       status['time_stamp'],
                                                                       (t1 - t0).total_seconds(),
                                                                       count_docs(status['snap_map'])))

                                    fp.flush();
                                break
                            else:
                                i += 1
                                if i > 3:
                                    if i%3==0:
                                      sys.stdout.write('.')

                                if (time.time() - t0) > int(ns.timeout):
                                    print ' [TIMED OUT]'
                                    failed += 1
                                    ex_status = "TIMED OUT"
                                    ex_time = time.time() - t0

                                    # Here is stop the pipeline when time out
                                    stop_url = server_uri + '/api/1/rest/pipeline/stop/' + prepared['runtime_id']
                                    stop_url = replaceString(stop_url)
                                    # print stop_url
                                    stop_resp = post(stop_url, {}, ns.username, ns.password)

                                    if fp:
                                        fp.write("\"%s\",%s,%s,%d,%d\n" % (pipe['name'],
                                                                           'TimedOut',
                                                                           status['time_stamp'],
                                                                           int(ns.timeout),
                                                                           0))
                                        fp.flush()
                                    break

                        except Exception, e:
                            print str(e);
                            break;

                else:
                    if 'error_map' in prepared:
                        sys.stdout.write("[Prepare failed] ")
                        if 'snap' in prepared['error_map']:
                            for snap in prepared['error_map']['snap']:

                                if 'error_msg' in prepared['error_map']['snap'][snap]:
                                    sys.stdout.write(
                                        json.dumps(prepared['error_map']['snap'][snap]['error_msg']) +
                                        "  ")
                                else:
                                    sys.stdout.write(json.dumps(prepared['error_map']['snap'][snap]))

                            sys.stdout.write("\n")
                        else:
                            try:
                                sys.stdout.write(json.dumps(prepared['error_map']['error_msg']));
                            except Exception, e:
                                sys.stdout.write(json.dumps(prepared['error_map']));
                    else:
                        print "[Prepare failed] %s" % json.dumps(prepared)
                    if fp:
                        fp.write("\"%s\",%s,%s,%d,%d\n" % (pipe['name'], 'Prepare Failed', '', 0, 0))
                    prepare_failed += 1
                    ex_status = "Prepare failed"
                    print time.time(),t0
                    ex_time = time.time() - t0
            pipe["status"] = ex_status
            pipe["completed time"] = ex_time
            total_ex_time += ex_time
            executed_pipelines.append(pipe)
        print "======================================================"
        print "Success %d  Failed %d  PrepareFailed %d total execution time %.3f" % (success, failed, prepare_failed,total_ex_time)

    else:
        print "Theier is no Standard Pipelines"

    return executed_pipelines


# Run Mapreduce Pipelines
def run_MapreducePipelines():
    global ns, constructList, server_uri
    if constructList["Mapreduce"]:
        Pipelines = constructList["Mapreduce"]
        success = 0
        failed = 0
        prepare_failed = 0
        total = 0
        # MR file created
        if ns.MRFile == "":
            outfileMR = createOutFile("map-reduce")
            ns.MRFile = outfileMR;
            if ns.MRFile:
                try:
                    fp = open(ns.dirname + "/" + ns.MRFile, 'w+')
                    print 'mapreduce Output sent to ==> %s\n' % ns.MRFile
                    fp.write('Pipeline Name,Status,StageName,StageStatus\n')
                except Exception, e:
                    pass;

        for pipe in Pipelines:
            if 'target_runtime' in pipe and pipe['target_runtime'] == "map-reduce":
                print pipe

    else:
        print "Theier is no Map Reduce Pipelines"

def createOutFile(str):
    global start_date,ns
    if not os.path.exists(ns.dirname):
        os.makedirs(ns.dirname)

    if '<std>' == ns.outfile:
        outfile = "Execute Project - %s - %d.%d.%d %d.%d.%d - %s - %s .csv" % (ns.project, start_date.year,
                                                                          start_date.month,
                                                                          start_date.day,
                                                                          start_date.hour,
                                                                          start_date.minute,
                                                                          start_date.second,
                                                                          ns.pod_uri,
                                                                          str)
    else:
        outfile = ns.outfile
    return outfile


def count_docs(stats):
    max_docs = 0
    for snap in stats:
        try:
            for view in stats[snap]['statistics']['input_views']:
                if stats[snap]['statistics']['input_views'][view]['documents_count'] > max_docs:
                    max_docs = stats[snap]['statistics']['input_views'][view]['documents_count']
        except Exception, e:
            pass;
        try:
            for view in stats[snap]['statistics']['input_views']:
                if stats[snap]['statistics']['input_views'][view]['documents_count'] > max_docs:
                    max_docs = stats[snap]['statistics']['input_views'][view]['documents_count']
        except Exception, e:
            pass;
    return max_docs

if __name__ == '__main__':
  main()
