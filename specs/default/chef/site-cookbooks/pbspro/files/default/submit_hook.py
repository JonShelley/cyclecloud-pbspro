# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#
'''
Note - the pbs module isn't very pythonic, so you'll see things like
    value = job.Resource_List["attribute"] or 100
instead of
    value = job.Resource_List.get("attribute", 100)
That is because is a metaclass, not a dict.

Also, place and select objects use repr() to convert to a parseable string, but
so you'll see guards against repr(None) (combined with the above) and 

Quick start:
    qmgr -c "create hook cycle_sub_hook"
    qmgr -c "set hook cycle_sub_hook event = queuejob"

    # reload source / config
    qmgr -c "import hook cycle_sub_hook application/x-python default submit_hook.py"
    qmgr -c "import hook cycle_sub_hook application/x-config default submit_hook.json"

See PBS Professional Programmers Guide for detailed information.

See /var/spool/pbs/server_logs/* for log messages
'''

from collections import OrderedDict
import json
import sys
import traceback
import os
import subprocess
import json

try:
    import pbs
except ImportError:
    import mockpbs as pbs


def validate_groupid_placement(job):
    '''
        @return True if the job has a placement group of group_id
        Note we will set it to group_id if it isn't specified.
    '''
    place = repr(job.Resource_List["place"]) if job.Resource_List["place"] else ""
    status, mj_place = get_groupid_placement(place)
    if mj_place != None:
        job.Resource_List["place"] = pbs.place(mj_place)
    return status
        

def get_groupid_placement(place):
    debug("Get groupid placement: %s" % place)
    placement_grouping = None
    for expr in place.split(":"):
        placement_grouping = None
        if "=" in expr:
            key, value = [x.lower().strip() for x in expr.split("=", 1)]
            if key == "group":
                placement_grouping = value
    if placement_grouping is None:
        debug("The user didn't specify place=group, setting group=group_id")
        placement_grouping = "group_id"
        prefix = ":" if place else ""
        mj_place = place + prefix + "group=group_id"
        return [True, mj_place]
    if placement_grouping == "group_id":
        return [True, None]
    else:
        debug("User specified a placement group that is not group_id - skipping.")
        return [False, None]


def parse_select(job, select_str=None):
    # 3:ncpus=2:slot_type=something
    select_toks = get_select_expr(job).split(":")
    select_N = int(select_toks[0])
    return select_N, OrderedDict([e.split("=", 1) for e in select_toks[1:]])


def parse_select_statement(select_str):
    # 3:ncpus=2:slot_type=something
    debug("parse select statement: %s" % select_str)
    select_toks = select_str.split(":")
    select_N = int(select_toks[0])
    return select_N, OrderedDict([e.split("=", 1) for e in select_toks[1:]])


def get_select(job):
    return job.Resource_List["select"]


def get_select_expr(job):
    return repr(get_select(job))


def append_select_expr(job, key, value):
    select_expr = get_select_expr(job)
    prefix = ":" if select_expr else ""
    job.Resource_List["select"] = pbs.select(select_expr + "%s%s=%s" % (prefix, key, value))


def append_select_statement(select_str, key, value):
    debug("append select statement: %s" % select_str)
    debug("Key: %s\tValue: %s" % (key, value))
    prefix = ":" if select_str else ""
    select_str = select_str + "%s%s=%s" % (prefix, key, value)
    debug("append select statement 2: %s" % select_str)
    return select_str


def set_select_key(job, key, value):
    select_expr = get_select_expr(job)
    key_values = select_expr.split(":")

    found = False

    for i in range(1, len(key_values)):
        possible_key, _ = key_values[i].split("=", 1)
        if possible_key == key:
            found = True
            key_values[i] = "%s=%s" % (key, value)
    
    if not found:
        append_select_expr(job, key, value)
    else:
        job.Resource_List["select"] = pbs.select(":".join(key_values))


def set_select_statement_key(select_str, key, value):
    debug("set select statement: %s" % select_str)
    debug("Key: %s\tValue: %s" % (key, value))
    key_values = select_str.split(":")

    found = False

    for i in range(1, len(key_values)):
        possible_key, _ = key_values[i].split("=", 1)
        if possible_key == key:
            found = True
            key_values[i] = "%s=%s" % (key, value)
    
    if not found:
        append_select_statement(select_str, key, value)
    else:
        select_str = ":".join(key_values)

    debug("set select statement 2: %s" % select_str)
    return select_str


def placement_hook(hook_config, job):

    if not get_select(job):
        # pbs 18 seems to treat host booleans as strings, which is causing this very annoying workaround.
        #job.Resource_List["ungrouped"] = "true"
        if job.Resource_List["slot_type"]:
            job.Resource_List["slot_type"] = job.Resource_List["slot_type"]
        debug("The job doesn't have a select statement, it doesn't have any placement requirements.")
        debug("Place a hold on the job")
        job.Hold_Types = pbs.hold_types("so")
        return

    if validate_groupid_placement(job):
        _, select_dict = parse_select(job)
        
        if "ungrouped" not in select_dict:
            set_select_key(job, "ungrouped", "false")
  
        slot_type = select_dict.get("slot_type")
        if slot_type:
            set_select_key(job, "slot_type", slot_type)
            debug("Using the grouped slot_type as a resource (%s)." % slot_type)


def debug(msg):
    pbs.logmsg(pbs.EVENT_DEBUG, "cycle_sub_hook - %s" % msg)


def error(msg):
    pbs.logmsg(pbs.EVENT_ERROR, "cycle_sub_hook - %s" % msg)

# another non-pythonic thing - this can't be behind a __name__ == '__main__',
# as the hook code has to be executable at the load module step.
hook_config = {}
if pbs.hook_config_filename:
    with open(pbs.hook_config_filename) as fr:
        hook_config.update(json.load(fr))
try:
    e = pbs.event()
    if e.type == pbs.QUEUEJOB:
        j = e.job
        placement_hook(hook_config, j)
    elif e.type == pbs.PERIODIC:
        # Defined paths to PBS commands
        qselect_cmd = os.path.join(pbs.pbs_conf['PBS_EXEC'], 'bin', 'qselect')
        qstat_cmd = os.path.join(pbs.pbs_conf['PBS_EXEC'], 'bin', 'qstat')
        qalter_cmd = os.path.join(pbs.pbs_conf['PBS_EXEC'], 'bin', 'qalter')
        qrls_cmd = os.path.join(pbs.pbs_conf['PBS_EXEC'], 'bin', 'qrls')
        # Get the jobs in an "so" hold state
        cmd = [qselect_cmd, "-h", "so"]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            debug('qselect failed!\n\tstdout="%s"\n\tstderr="%s"' % (stdout, stderr))
        jobs = stdout.split()
        debug("Jobs: %s" % jobs)
        # Get the job information
        cmd = [qstat_cmd, "-f", "-F", "json"] + jobs[:25]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            debug('qselect failed!\n\tstdout="%s"\n\tstderr="%s"' % (stdout, stderr))
        #debug(stdout)
        qstat_json = json.loads(stdout)
        jobs = qstat_json["Jobs"]
        for key, value in jobs.iteritems():
            # Reevaluate each held job
            debug("Key: %s\nValue: %s" % (key, value))
            j_place = jobs[key]["Resource_List"]["place"]
            j_select = jobs[key]["Resource_List"]["select"]
            # Check the groupid placement
            mj_place = None
            status, mj_place = get_groupid_placement(j_place)
            if status:
                mj_select = None
                _, select_dict = parse_select_statement(j_select)
                if "ungrouped" not in select_dict:
                    mj_select = set_select_statement_key(j_select, "ungrouped", "false")
  
                slot_type = select_dict.get("slot_type")
                if slot_type:
                    mj_select = set_select_statement_key(mj_select, "slot_type", slot_type)
                    debug("Using the grouped slot_type as a resource (%s)." % slot_type)
                # Qalter the job
                if mj_select != None:
                    debug("New select statement: %s" % mj_select)
                if mj_place != None:
                    debug("New place statement: %s" % mj_place)
                debug("qalter the job")

                # Release the hold on the job
                debug("Release the hold on the job")
                
                
            
except:
    error(traceback.format_exc())
    raise
