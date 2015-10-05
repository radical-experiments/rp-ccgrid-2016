import os
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
    else:
        # Default
        access = None

    report.info("Collecting profiles for session: %s.\n" % sid)
    sid_profiles = rpu.fetch_profiles(sid=sid, client=CLIENT_DIR,
                                      tgt=TARGET_DIR, access=access,
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

    sessions_to_fetch = ['rp.session.netbook.mark.016708.0003',
                     'rp.session.netbook.mark.016708.0004',
                     'rp.session.netbook.mark.016708.0005',
                     'rp.session.netbook.mark.016708.0010',
                     'rp.session.netbook.mark.016708.0011',
                     'rp.session.netbook.mark.016708.0012'
    ]

    collect_all(sessions_to_fetch)
