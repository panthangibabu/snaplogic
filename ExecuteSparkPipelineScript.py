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

DEFAULT_USER = "pbabu@snaplogic.com" # Replace with valid username or pass it from jenkins as a parameter
DEFAULT_PW   = "Babu@999" #this passwd is not a valid one, either give a valid passwd or pass it from jenkins 
DEFAULT_ORG  = 'snapreduce'
DEFAULT_PLEX = 'snapreduce/rt/sidekick/HDP2.3.4_Plex'
DEFAULT_PLEXNAME = 'QA_HDP2.3.4_Plex'
DEFAULT_PS = 'HDFS Pipelines'
DEFAULT_PROJECT = 'HDFS 4.6'
DEFAULT_POD_URI = 'budgy.elastic.snaplogic.com'
DEFAULT_MAX_PIPELINE_EXECUTION_TIME = 300    # in seconds
DEFAULT_REST_TIMEOUT = 120 # timeout on control plane requests

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument('--thread-count', help=('The number of threads to be started. Each '
                                                'thread will sequentially execute its pipelines'),
                        default=1, type=int)

    parser.add_argument('--pod-uri', type=str, help='The hostname of the pod you are running on. '
                        'Must match the prefix of the account files',
                        default=DEFAULT_POD_URI)
    parser.add_argument('--projectspace', type=str, help='ProjectSpace where project resides', default=DEFAULT_PS)
    parser.add_argument('--project', default=DEFAULT_PROJECT, type=str, help='Project to execute')
    parser.add_argument('--plex-name', type=str, help='The runtime path to execute on.  '
                                                      'EX: snaplogic/rt/cloud/dev',
                        default=DEFAULT_PLEX)
    parser.add_argument('--plex', type=str, help='Plex name as displayed in plex dropdown in designer or from dashboard '
                                                      'EX: DevCluster',
                        default=DEFAULT_PLEXNAME)
    parser.add_argument('--username', type=str, help='Username to run pipelines as',
                        default=DEFAULT_USER)

    parser.add_argument('--password', type=str, help='Password to use', default=DEFAULT_PW)

    parser.add_argument('--org', type=str, help='Org to use', default=DEFAULT_ORG)

    parser.add_argument('--outfile', type=str, help='Result file', default='<std>')

    parser.add_argument('--timeout', type=str, help='Pipeline execution timeout', default=DEFAULT_MAX_PIPELINE_EXECUTION_TIME)

    ns = parser.parse_args()
    
    print 'Executing pipelines with passwd %s and timeout as %s and plex as %s' % (ns.password, ns.timeout, ns.plex_name)

    server_uri = 'https://' + ns.pod_uri

    start_date = datetime.datetime.utcnow()

    # org_id of selected org
    org_rest_api=server_uri + '/api/1/rest/asset/user/' + ns.username
    org_rest_api=replaceString(org_rest_api)
    print "org_rest_api ==>"+org_rest_api
    user = get(org_rest_api, ns.username, ns.password)
    orglist=user.get("org_snodes")
    for key in orglist:
        orgname=orglist.get(key).get("name")
        if orgname == ns.org:
            org_id = key
    print "**********************************"
    print 'org_id : %s' % org_id
    print "*********************************"
    print 'Executing pipelines in project %s on %s as %s' % (ns.project, ns.pod_uri, ns.username)
    print 'Start time: %s' % datetime.datetime.utcnow()
    dirname="Result"
    if not os.path.exists(dirname):
       os.makedirs(dirname)

    if '<std>' == ns.outfile:
        outfile = "Execute Project - %s - %d.%d.%d %d.%d.%d - %s.csv" % (ns.project,start_date.year,
                                                  start_date.month,
                                                  start_date.day,
                                                  start_date.hour,
                                                  start_date.minute,
                                                  start_date.second,
                                                  ns.pod_uri)
    else:
        outfile = ns.outfile
        

    if outfile:
        try:
            fp = open(dirname+"/"+outfile,'w+')
            print 'Output sent to %s\n' % outfile
            fp.write('Pipeline Name,Status,StageName,StageStatus\n')
        except Exception, e:
            pass;

    # Get list of pipelines
    pipelines_rest_api=server_uri + '/api/1/rest/asset/list/' + ns.org + '/'+ ns.projectspace +'/' +ns.project + '?asset_type=Pipeline'
    pipelines_rest_api=replaceString(pipelines_rest_api)
    pipelines = get(pipelines_rest_api, ns.username, ns.password)
    print 'list of pipelines api call is successful'
    #print pipelines['entries']
    success = 0
    failed = 0
    prepare_failed = 0

    for pipe in pipelines['entries']:
        print 'metadata :%s' % pipe['metadata']

	if 'target_runtime' in pipe['metadata']['target_runtime']:
           print 'target_runtime :%s' % pipe['metadata']['target_runtime']
        else:
           print 'target_runtime is empty'

	#sys.exit()
        # prepare pipeline
        sys.stdout.write("%-50s  " % pipe['name'][:49])
        print "******************************"
        prepare_url=server_uri + '/api/1/rest/pipeline/prepare/' + pipe['snode_id']+'?target_runtime=spark'
        #print 'prepare_url :%s' % prepare_url
        prepare_url=replaceString(prepare_url)
        print 'prepare_url :%s' % prepare_url
        prepared = post(prepare_url,
                        {'runtime_path_id' : ns.plex_name,"runtime_label":ns.plex,"do_start":True,"async":True,"priority":10,"queueable":True}, ns.username, ns.password)
        print 'prepare_url :%s' % prepared

        if (prepared and 'runtime_id' in prepared):
            print 'org id is %s and runtime id is %s' % (org_id,prepared['runtime_id'])
            sys.stdout.write("Prepared, Start Executing ");
            i = 0;
            t0 = time.time();
            while 1:
                time.sleep(2)
                try:
                    status = get(server_uri + '/api/2/' + org_id + '/rest/pm/runtime/' +
                                 prepared['runtime_id'] + '?level=detail',
                                 ns.username, ns.password)
                    if 'state' in status and status['state'] == 'Failed' and status.get("spark_metrics").get('error_msg') != None:
                        errormsg='Failed: error msg is :- ' + status.get("spark_metrics").get('error_msg')
                        if fp:
                                    fp.write("\"%s\",%s,%s,%s\n" % (pipe['name'],
                                            errormsg,'',''))
                                    fp.flush()
                    elif 'state' in status and (status['state'] == 'Failed' or status['state'] == 'Stopped' or status['state'] == 'Completed'):
                        job_spark_metrics=status.get("spark_metrics").get("jobs")
                        for job in job_spark_metrics:
                            stages=job_spark_metrics.get(job).get('stages')
                            stagestatus=job_spark_metrics.get(job).get('status')
                            for stage in stages:
                                name=stages.get(stage).get('name')
                                if fp:
                                    fp.write("\"%s\",%s,%s,%s\n" % (pipe['name'],
                                            status['state'],name,stagestatus))
                                    fp.flush()
                    if 'state' in status and (status['state'] == 'Failed' or status['state'] == 'Stopped'):
                        print '   [' + status['state'] + ']'
                        failed += 1
                        break

                    elif 'state' in status and status['state'] == 'Completed':
                        print '   [' + status['state'] + ']'
                        success += 1
                        break
                    else:
                        i += 1
                        if i > 3:
                            sys.stdout.write('.')

                        if (time.time() - t0) >  int(ns.timeout):
                            print ' [TIMED OUT]'
                            failed += 1
                            if fp:
                                fp.write("\"%s\",%s,%s,%s\n" % (pipe['name'],
                                                            'TimedOut','',''))
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
                                json.dumps(prepared['error_map']['snap'][snap]['error_msg'])+
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
                fp.write("\"%s\",%s,%s,%s\n" % (pipe['name'],'Prepare Failed','',''))
            prepare_failed += 1
    print "======================================================"
    print "ExecutionSuccess %d  ExecutionFailed %d  PrepareFailedDuringExecution %d" % (success, failed, prepare_failed)

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
        out= str(e);
    except urllib2.HTTPError, e:
        out = str(e); 
        #"%s %s" % (e.code, e.reason)
    except Exception, e:
        out = str(e) 
    if 'response_map' in out:
        return out['response_map']


    return out

def replaceString(string):

   # replace the space with '%20' in string
   string=string.replace(" ", "%20")
   return string


if __name__ == '__main__':
    main()
