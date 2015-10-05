import os
import sys
import radical.pilot.utils as rpu
import radical.utils as ru
import pandas as pd

EXPERIMENT_DIR = '/Users/mark/ipdps16/experiments'

CLIENT_DIR = os.path.join(EXPERIMENT_DIR, 'bin')
TARGET_DIR = os.path.join(EXPERIMENT_DIR, 'data/profiling')
JSON_DIR = os.path.join(EXPERIMENT_DIR, 'data/json')

RADICAL_PILOT_DBURL = None

###############################################################################
#
def collect(sid):

    # If we run from the titan headnode, collect over GO
    if 'rp.session.titan' in sid:
        access = 'go://olcf#dtn'
        client_dir = 'go://olcf#dtn/ccs/home/marksant1/ipdps16/bin'
        target_dir = 'go://localhost%s' % TARGET_DIR
    else:
        # Default
        access = None
        client_dir = CLIENT_DIR
        target_dir = TARGET_DIR

    report.info("Collecting profiles for session: %s.\n" % sid)
    sid_profiles = rpu.fetch_profiles(sid=sid, client=client_dir,
                                      tgt=target_dir, access=access,
                                      skip_existing=True)
    print sid_profiles

    report.info("Collecting json for session: %s.\n" % sid)
    rpu.fetch_json(sid, tgt=JSON_DIR, skip_existing=True)


###############################################################################
#
def collect_all(sessions_to_fetch):

    for sid in sessions_to_fetch:
        collect(sid)


###############################################################################
#
if __name__ == '__main__':

    report = ru.Reporter("Collect profiling and json data to local disk.")

    session_ids = []

    # Read from file if specified, otherwise read from stdin
    f = open(sys.argv[1]) if len(sys.argv) > 1 else sys.stdin
    for line in f:
        session = line.strip()
        if session:
            session_ids.append(session)

    report.info("Session ids found on input:\n")
    report.plain("%s\n" % session_ids)

    collect_all(session_ids)
