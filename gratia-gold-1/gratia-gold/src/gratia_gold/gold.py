
"""
Module for interacting with Gold

Takes a summarized Gratia job and either charges or refunds it.
"""

import os
import pwd
import errno
import logging
from datetime import datetime, timedelta

log = logging.getLogger("gratia_gold.gold")

def setup_env(cp):
    gold_home = cp.get("gold", "home")
    if not os.path.exists(gold_home):
        raise Exception("GOLD_HOME %s does not exist!" % gold_home)
    os.environ['GOLD_HOME'] = gold_home
    paths = os.environ['PATH'].split(";")
    paths.append(os.path.join(gold_home, "bin"))
    paths.append(os.path.join(gold_home, "sbin"))
    # join the elements in paths by ;
    os.environ['PATH'] = ";".join(paths)
    
def drop_privs(cp):
    gold_user = cp.get("gold", "username")
    pw_info = pwd.getpwnam(gold_user)
    try:
        os.setgid(pw_info.pw_gid)
        os.setuid(pw_info.pw_uid)
    except OSError, oe:
        # errno.EPERM (Operation not permitted)
        if oe.errno != errno.EPERM:
            raise
        log.warn("Unable to drop privileges to %s - continuing" % gold_user)


def get_digits_from_a_string(string1):
    if string1 is None:
        return 1
    if (type(string1) is int) or (type(string1) is long):
        return string1
    print string1
    digitsofstring1 = ""
    for i in range(len(string1)):
        if string1[i]>='0' and string1[i]<='9':
            digitsofstring1 += digitsofstring1[i]
    if digitsofstring1 == "":
        numberofstring1 = 1
    else:
        numberofstring1 = int(digitsofstring1)
    return numberofstring1


'''
Modified by Yaling Zheng
job has the following information 
dbid, resource_type, vo_name, user, charge, wall_duration, cpu, node_count, njobs, 
processors, endtime, machine_name, project_name

2012-05-09 20:19:46 UTC [yzheng@osg-xsede:~/mytest]$ gcharge -h
Usage:
    gcharge [-u user_name] [-p project_name] [-m machine_name] [-C
    queue_name] [-Q quality_of_service] [-P processors] [-N nodes] [-M
    memory] [-D disk] [-S job_state] [-n job_name] [--application
    application] [--executable executable] [-t charge_duration] [-s
    charge_start_time] [-e charge_end_time] [-T job_type] [-d
    charge_description] [--incremental] [-X | --extension property=value]*
    [--debug] [-?, --help] [--man] [--quiet] [-v, --verbose] [-V, --version]
    [[-j] gold_job_id] [-q quote_id] [-r reservation_id] {-J job_id}

'''
def call_gcharge(job, logfile):

    args = []
    #args += ["-u", job['user']]
    # force the user name to be yzheng
    job['user'] = "yzheng"
    args += ["-u", "yzheng"]
    #if job['project_name']:
    #    args += ["-p", job['project_name']]
    # force the project name to be OSG-Staff
    job['project_name'] = "OSG-Staff"
    args += ["-p", "OSG-Staff"]
    #args += ["-m", job["machine_name"]]
    # force the machine name to be grid1.osg.xsede
    job['machine_name'] = "grid1.osg.xsede"
    args += ["-m", "grid1.osg.xsede"] # force the machine name to be grid1.osg.xsede
    
    originalnumprocessors = job['processors']
    job['processors'] = get_digits_from_a_string(originalnumprocessors)
    #args += ["-P", job['processors']]
    
    originalnodecount = job['node_count']
    job['node_count'] = get_digits_from_a_string(originalnodecount)
    #args += ["-N", job['node_count']]]
    # ignore the job endtime, the default end time is now
    # force the end time to be the day after tomorrow
    if job['endtime'] is None:
        today = datetime.today()
        dt = datetime(today.year, today.month, today.day, today.hour, today.minute, today.second)
        job['endtime'] = str(dt+timedelta(1,0)) # now + 24 hours
    # args += ["-e", job['endtime']]
    if job['dbid']:
        args += ["-J", job['dbid']]
    
    # [-t charge_duration]
    # 'charge' is a must option
    if job['charge'] is None:
        job['charge'] = 3600 # default 3600 seconds, which is 1 hour

    mystring = "gcharge -u " + str(job['user']) \
        + " -p "+str(job['project_name']) \
        + " -m " + str(job['machine_name']) \
        + " -N " + str(job['node_count']) \
        + " -P "+str(job['processors']) \
        + " -e \"" + str(job['endtime'])+"\"" \
        + " -J " + str(job['dbid']) \
        + " -t " + str(job['charge'])
    print mystring
    try:
        gchargeexitstatus = os.system(mystring)
    except:
        print "gcharge failed ... "
    print "gcharge return status is " + str(gchargeexitstatus)
    return gchargeexitstatus
    #args += ["-t", job['charge']]
    #print "gcharge args"
    #print args
    # no queue name?
    # quality_of_service ?
    # nodes?
    # memory? I guess no
    # disk, I guess no
    # job_state, I guess no
    # job_name, I guess no
    # application, I guess no
    # executable, I guess no
    # charge_duration, I guess no
    # charge_start_time, I guess no, maybe default is now
    # charge_end_time is included
    # job_type, I guess no
    # charge_description, I guess no
    # --incremental, I guess no
    # --debug, I guess no
    # -?, --help, --man --quiet, --verbose, --version
    # gold_job_id, I guess no
    # quota_id, I guess no
    # reservation_id, I guess no
    # job_id, I guess no
    #raise NotImplementedError()
    # fd = open(logfile, "w")
    # if fd:
    #     try:
    #         os.system();
    #         os.execv("gcharge", args)
    #         print "gcharge succeed ......."
    #     except:
    #         print "gcharge failed ...... "
    #         return -1
    # else:
    #     return -1

    # pid = os.fork()
    # fd = open(logfile, "w")
    # if pid == 0:
    #     os.dup2(fd.fileno, 1)
    #     os.dup2(fd.fileno, 2)
    #     try:
    #         os.execv("gcharge", args)
    #     except:
    #         return -1
    # pid2 = 0
    # while pid != pid2:
    #     pid2, status = os.wait()
    # return status

def refund(cp, job, logfile):
    # print "grefund"
    # args = []
    # # [-j] gold_job_id, we assume dbid is unique, and regard it as gold_job_id
    # # I think dbid is a must option
    #args += ["-j", job["dbid"]]
    strrefund = "grefund -J "+str(job['dbid'])
    try:
        os.system(strrefund)
    except:
        print "job refund failed ... \n"
    # print "grefund args"
    # print args
    # fd = open(logfile, "w")
    # if fd:
    #     try:
    #         os.execv("grefund", args)
    #         print "grefund succeed ...... "
    #     except:
    #         print "grefund failed ...... "
    #         return -1
    # else:
    #     return -1

    # pid = os.fork()
    # fd = open(logfile, "w")
    # if pid == 0:
    #     os.dup2(fd.fileno, 1)
    #     os.dup2(fd.fileno, 2)
    #     os.execv("grefund", args)
    # pid2 = 0
    # while pid != pid2:
    #     pid2, status = os.wait()
    # return status

