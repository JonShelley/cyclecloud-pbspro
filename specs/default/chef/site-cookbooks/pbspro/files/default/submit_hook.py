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


def get_groupid_placement(job):
    '''
        @return True if the job has a placement group of group_id
        Note we will set it to group_id if it isn't specified.
    '''
    place = repr(job.Resource_List["place"]) if job.Resource_List["place"] else ""
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
        job.Resource_List["place"] = pbs.place(place + prefix + "group=group_id")
        
    if placement_grouping == "group_id":
        return [True, ""]
    else:
        debug("User specified a placement group that is not group_id - skipping.")
        return [False, ""]
        

def placement_hook(hook_config, job):

    # Check for a place request
    status, place_str = get_groupid_placement(job)


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
            
except:
    error(traceback.format_exc())
    raise
