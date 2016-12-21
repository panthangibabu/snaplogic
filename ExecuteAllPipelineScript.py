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
#Plexes defin
#standared plex
DEFAULT_PLEXNAME = ''
DEFAULT_PLEX = '' #'snapreduce/rt/sidekick/HDP2.3.4_Plex'
#SPark plex
DEFAULT_SPARK_PLEXNAME = ''
DEFAULT_SPARK_PLEX =''# 'snapreduce/rt/sidekick/HDP2.3.4_Plex'

DEFAULT_PS = '' #project Space
DEFAULT_PROJECT =''  #Project
DEFAULT_POD_URI = '' #'snap.elastic.snaplogic.com'
DEFAULT_MAX_PIPELINE_EXECUTION_TIME = 300    # in seconds
DEFAULT_REST_TIMEOUT = 120 # timeout on control plane requests

#Filter
DEFAULT_FILTER= ''

#global variables
server_uri=""
ns={}
plexList={}


start_date = datetime.datetime.utcnow()

def main():
    global server_uri, ns
    parser = argparse.ArgumentParser()

    parser.add_argument('--thread-count', help=('The number of threads to be started. Each '
                                                'thread will sequentially execute its pipelines'),
                        default=1, type=int)

    parser.add_argument('--pod-uri', type=str, help='The hostname of the pod you are running on. '
                                                    'Must match the prefix of the account files',
                        default=DEFAULT_POD_URI)
    parser.add_argument('--projectspace', type=str, help='ProjectSpace where project resides', default=DEFAULT_PS)
    parser.add_argument('--project', default=DEFAULT_PROJECT, type=str, help='Project to execute')
    parser.add_argument('--plex-rid', type=str, help='The runtime path to execute on.  '
                                                      'EX: snaplogic/rt/cloud/dev',
                        default=DEFAULT_PLEX)
    parser.add_argument('--plex', type=str,
                        help='Plex name as displayed in plex dropdown in designer or from dashboard '
                             'EX: DevCluster',
                        default=DEFAULT_PLEXNAME)

    parser.add_argument('--spark-plex-rid', type=str, help='The runtime path to execute on.  '
                                                      'EX: snaplogic/rt/cloud/dev',
                        default=DEFAULT_SPARK_PLEX)
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

    parser.add_argument('--filter', type=str, help='Filter expression', default=DEFAULT_FILTER)

    ns = parser.parse_args()
    #print ns

    # Here Check the spark plex is empty or not , If empty put both use same plex
    if not bool(ns.spark_plex) and not bool(ns.spark_plex):
        print "Provide any plex name to run pipelines"
    if not bool(ns.spark_plex):
        ns.spark_plex = ns.plex
    else:
        ns.plex = ns.spark_plex

    filter_list=[]
    ns.filter_list=ns.filter.split(",")
    # print ns
    # sys.exit()

    server_uri = 'https://' + ns.pod_uri

    # org_id of selected org
    org_id=getOrgId()
    print "**********************************"
    print 'org_id : %s' % org_id
    print "**********************************"

#----------------------------------------------create files and result ------------------------
    # add the directory ns it self

   #ns.dirname="%d.%d.%d" % (start_date.year,start_date.month,start_date.day)

    ns.dirname= "%s - %d.%d.%d" % (ns.project,start_date.year,start_date.month,start_date.day)
   #print ns.dirname


#------------------------------------------------standard file-------------------
    ns.standardFile=""
# ------------------------------------------------spark file-------------------
    ns.sparkFile =""
#----------------------------------------------MR File---------------------------
    ns.MRFile = ""
#----------------------------------------------create files and exit----------------------------


#--------------------------------------plexes----------------------
    ns.plex_rid=getPlexRID(ns.plex)
    ns.spark_plex_rid=getPlexRID(ns.spark_plex)
    #print ns

    print 'Executing pipelines with \n\t user as %s \n\t timeout as %s \n\t plex as %s \n\t Hadoop plex as %s \n \t' % (
    ns.username, ns.timeout, ns.plex,ns.spark_plex)
#--------------------------------------------------------plex end---------------



#----------------------------------Pipelines sarted-----------------------------------
    runPipelines()
# ----------------------------------Pipelines ended-----------------------------------





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

#newly added def ----------------------------------------------------------------------------------------
def replaceString(string):

        # replace the space with '%20' in string
        string = string.replace(" ", "%20")
        return string

#get the all Orgs for given user
def getAllOrgs():
    global server_uri,ns
    org_rest_api = server_uri + '/api/1/rest/asset/user/' + ns.username
    org_rest_api = replaceString(org_rest_api)
    print "\n org_rest_api ==>" + org_rest_api
    user = get(org_rest_api, ns.username, ns.password)
    orglist = user.get("org_snodes")
    return orglist

def getOrgId():
    orglist=getAllOrgs()
    #print orglist
    for key in orglist:
        orgname = orglist.get(key).get("name")
        if orgname == ns.org:
            org_id = key
    #print 'org_id : %s' % org_id
    ns.org_id=org_id
    return org_id

#get the all Orgs for given user
def getAllPlexes():
    global server_uri,ns
    plex_rest_api = server_uri + '/api/1/rest/plex/org/'+ ns.org
    plex_rest_api = replaceString(plex_rest_api)
    print "\n org_rest_api ==>" + plex_rest_api
    plexList={}
    plexs= get(plex_rest_api,ns.username,ns.password)
    #print plexList
    for plex in plexs:

         #print plex["label"] + " = " + plex["path"] + " = " + plex["runtime_path_id"] + "\n"
         plexList[plex["label"]]=plex["runtime_path_id"]
    return plexList

def getPlexRID(plexname):
    global plexList
    if not bool(plexList):
     plexList=getAllPlexes()

    #print plexList
    plexRId=""
    for key in plexList:
        if key == plexname:
         plexRId = plexList.get(key)
    #print 'plexRId : %s' % plexRId
    return plexRId

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


def runPipelines():
    global server_uri, ns
    pipelinesList = getAllPipelines()
    success = 0
    failed = 0
    prepare_failed = 0
    #here vars for spark mode
    spark_success = 0
    spark_failed = 0
    spark_prepare_failed = 0
    spark_total=0
    #here vars for standard mode
    standard_success = 0
    standard_failed = 0
    standard_prepare_failed = 0
    standard_total = 0
    # here vars for map reduse mode
    mr_success = 0
    mr_failed = 0
    mr_prepare_failed = 0
    mr_total = 0

    #---------------------------------------------------------------------------------------------
    #Here create the files

    #Spark file created
    if ns.sparkFile == "":
        outfileSpark = createOutFile("spark")
        ns.sparkFile = outfileSpark;
        if ns.sparkFile:
            try:
                sparkFile = open(ns.dirname + "/" + ns.sparkFile, 'w+')
                print 'Spark Mode Output sent to ==> %s\n' % ns.sparkFile
                sparkFile.write('Pipeline Name,Status,StageName,StageStatus\n')
            except Exception, e:
                pass;

    # MR file created
    if ns.MRFile == "":
        outfileMR = createOutFile("map-reduce")
        ns.MRFile = outfileMR;
        if ns.MRFile:
            try:
                MRFile = open(ns.dirname + "/" + ns.MRFile, 'w+')
                print 'mapreduce Output sent to ==> %s\n' % ns.MRFile
                MRFile.write('Pipeline Name,Status,StageName,StageStatus\n')
            except Exception, e:
                pass;

    #Standard file created
    if ns.standardFile == "":
        outfileStandard = createOutFile("Standard")
        ns.standardFile = outfileStandard;
        if ns.standardFile:
            try:
                standardFile = open(ns.dirname + "/" + ns.standardFile, 'w+')
                print 'standard Output sent to ==> %s\n' % ns.standardFile
                standardFile.write('Pipeline Name,Status,StartTime,Duration,Documents\n')
            except Exception, e:
                pass;


    #---------------------------------------------------------------------------------------------

    for pipe in pipelinesList['entries']:
        # print 'metadata :%s' % pipe['metadata']
        if ns.filter_list and not any(x.lower() in pipe['name'].lower() for x in ns.filter_list):
            # ignore
            continue;


        if any('[x]' in pipe['name'].lower() for x in ns.filter_list):
            print pipe['name']
            continue;

        if 'target_runtime' in pipe:
            #print '\n ' + pipe["name"] + ' --- ' + 'target_runtime :%s' % pipe['target_runtime']
            if pipe['target_runtime'] == "spark" :
                #print "Spark run time"

                spark_total+=1
                sys.stdout.write("%-50s  " % pipe['name'][:49])
                sys.stdout.write("%-20s  " % pipe['target_runtime'])
                prepare_url = server_uri + '/api/1/rest/pipeline/prepare/' + pipe['snode_id'] + '?target_runtime=spark'
                # print 'prepare_url :%s' % prepare_url
                prepare_url = replaceString(prepare_url)
                #print 'prepare_url :%s' % prepare_url
                prepared = post(prepare_url,
                                {'runtime_path_id': ns.spark_plex_rid, "runtime_label": ns.spark_plex, "do_start": True,
                                 "async": True, "priority": 10, "queueable": True}, ns.username, ns.password)
                #print 'prepare_url :%s' % prepared

                if (prepared and 'runtime_id' in prepared):
                    #print 'org id is %s and runtime id is %s' % (ns.org_id, prepared['runtime_id'])
                    sys.stdout.write("Prepared, Start Executing ");
                    i = 0;
                    t0 = time.time();
                    while 1:
                        time.sleep(2)
                        try:
                            status = get(server_uri + '/api/2/' + ns.org_id + '/rest/pm/runtime/' +
                                         prepared['runtime_id'] + '?level=detail',
                                         ns.username, ns.password)
                            if 'state' in status and status['state'] == 'Failed' and status.get("spark_metrics").get(
                                    'error_msg') != None:
                                errormsg = 'Failed: error msg is :- ' + status.get("spark_metrics").get('error_msg')
                                if sparkFile:
                                    sparkFile.write("\"%s\",%s,%s,%s\n" % (pipe['name'],
                                                                           errormsg, '', ''))
                                    sparkFile.flush()
                            elif 'state' in status and (
                                                status['state'] == 'Failed' or status['state'] == 'Stopped' or status[
                                        'state'] == 'Completed'):
                                job_spark_metrics = status.get("spark_metrics").get("jobs")
                                for job in job_spark_metrics:
                                    stages = job_spark_metrics.get(job).get('stages')
                                    stagestatus = job_spark_metrics.get(job).get('status')
                                    for stage in stages:
                                        name = stages.get(stage).get('name')
                                        if sparkFile:
                                            sparkFile.write("\"%s\",%s,%s,%s\n" % (pipe['name'],
                                                                                   status['state'], name, stagestatus))
                                            sparkFile.flush()
                            if 'state' in status and (status['state'] == 'Failed' or status['state'] == 'Stopped'):
                                print '   [' + status['state'] + ']'
                                failed += 1
                                spark_failed+=1
                                break
                            elif 'state' in status and status['state'] == 'Completed':
                                print '   [' + status['state'] + ']'
                                success += 1
                                spark_success+=1
                                break
                            else:
                                i += 1
                                if i > 3:
                                    sys.stdout.write('.')

                                if (time.time() - t0) > int(ns.timeout):
                                    print ' [TIMED OUT]'
                                    failed += 1
                                    spark_failed+=1
                                    if sparkFile:
                                        sparkFile.write("\"%s\",%s,%s,%s\n" % (pipe['name'],
                                                                               'TimedOut', '', ''))
                                        sparkFile.flush()
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
                    if sparkFile:
                        sparkFile.write("\"%s\",%s,%s,%s\n" % (pipe['name'], 'Prepare Failed', '', ''))
                    prepare_failed += 1
                    spark_prepare_failed+=1


            elif pipe['target_runtime'] == "map-reduce" :
                print "Map Reduce"
                print '\n ' + pipe["name"] + ' --- ' + 'target_runtime :%s' % pipe['target_runtime']
                mr_total+=1
                sys.stdout.write("%-50s  " % pipe['name'][:49])
                sys.stdout.write("%-20s  \n" % pipe['target_runtime'])

        else:
            #print '\n ' + pipe["name"] + ' --- ' + 'target_runtime is empty'
            #print "Standared mode"

            standard_total+=1
            sys.stdout.write("%-50s  " % pipe['name'][:49])
            sys.stdout.write("%-20s  " % "Standared")

            prepare_url = server_uri + '/api/1/rest/pipeline/prepare/' + pipe['snode_id']
            #print "\n prepare_url =>" + prepare_url

            prepared = post(prepare_url,
                            {'runtime_path_id': ns.plex_rid, "runtime_label": ns.plex, "do_start": True, "async": True,
                             "priority": 10, "queueable": True}, ns.username, ns.password)
            # print prepared
            if (prepared and 'runtime_id' in prepared):

                sys.stdout.write("Prepared ")
                result = post(server_uri + '/api/1/rest/pipeline/start/' +
                              prepared['runtime_id'], {}, ns.username, ns.password)
                # Todo: Make sure it starts
                sys.stdout.write("Running ");
                i = 0;
                t0 = time.time();
                while 1:
                    time.sleep(2)
                    try:
                        status = get(server_uri + '/api/2/' + ns.org_id + '/rest/pm/runtime/' +
                                     prepared['runtime_id'] + '?level=detail',
                                     ns.username, ns.password)
                        if 'state' in status and (status['state'] == 'Failed' or status['state'] == 'Stopped'):
                            print '   [' + status['state'] + ']'
                            failed += 1
                            standard_failed+=1
                            if standardFile:
                                t0 = iso8601.parse_date(status['create_time']);
                                t1 = iso8601.parse_date(status['time_stamp']);
                                #print pipe['name'] + " 1\n"
                                standardFile.write("\"%s\",%s,%s,%d,%d\n" % (pipe['name'],
                                                                             status['state'],
                                                                             status['time_stamp'],
                                                                             (t1 - t0).total_seconds(),
                                                                             count_docs(status['snap_map'])))
                                standardFile.flush()
                            break

                        elif 'state' in status and status['state'] == 'Completed':
                            print '   [' + status['state'] + ']'
                            success += 1
                            standard_success+=1
                            if standardFile:
                                t0 = iso8601.parse_date(status['create_time']);
                                t1 = iso8601.parse_date(status['time_stamp']);
                                #print pipe['name'] + " 2\n"
                                standardFile.write("%s,%s,%s,%d,%d\n" % (pipe['name'],
                                                                         status['state'],
                                                                         status['time_stamp'],
                                                                         (t1 - t0).total_seconds(),
                                                                         count_docs(status['snap_map'])))

                                standardFile.flush();
                            break
                        else:
                            i += 1
                            if i > 3:
                                sys.stdout.write('.')

                            if (time.time() - t0) > int(ns.timeout):
                                print ' [TIMED OUT]'
                                failed += 1
                                standard_failed+=1
                                if standardFile:
                                    #print pipe['name'] + " 3\n"
                                    standardFile.write("\"%s\",%s,%s,%d,%d\n" % (pipe['name'],
                                                                                 'TimedOut',
                                                                                 status['time_stamp'],
                                                                                 int(ns.timeout),
                                                                                 0))
                                    standardFile.flush()
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
                if standardFile:
                    #print pipe['name'] + " 4\n"
                    standardFile.write("\"%s\",%s,%s,%d,%d\n" % (pipe['name'], 'Prepare Failed', '', 0, 0))
                prepare_failed += 1
                standard_prepare_failed+=1

    print "======================================================"
    print "ExecutionSuccess %d  ExecutionFailed %d  PrepareFailedDuringExecution %d \n" % (success, failed, prepare_failed)
    print "Spark-ExecutionSuccess %d  Spark-ExecutionFailed %d  Spark-PrepareFailedDuringExecution %d \n" % (
    spark_success, spark_failed, spark_prepare_failed)
    print "standard-ExecutionSuccess %d  standard-ExecutionFailed %d  standard-PrepareFailedDuringExecution %d \n" % (
        standard_success, standard_failed, standard_prepare_failed)
    print "Total Pipelines : %d \n" %(standard_total+mr_total+spark_total)



def getAllPipelines():
    global server_uri, ns
    pipelines_rest_api=server_uri + '/api/1/rest/asset/list/' + ns.org + '/'+ ns.projectspace +'/' +ns.project + '?asset_type=Pipeline'
    pipelines_rest_api=replaceString(pipelines_rest_api)
    print "\n pipelines_rest_api ==>" + pipelines_rest_api
    pipelinesList = get(pipelines_rest_api, ns.username, ns.password)
    print 'list of pipelines api call is successful \n'

    return pipelinesList

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


#ended ---------------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    main()
