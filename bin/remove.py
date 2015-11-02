import os
import sys
import glob
import radical.pilot.states as rps
import radical.pilot.utils as rpu
import radical.utils as ru
import pandas as pd

from common import JSON_DIR, TARGET_DIR, PICKLE_DIR, HDF5_DIR

###############################################################################
#
def remove_all(session_ids, storage):

    if storage == 'pickle':

        try:
            ses_info_fr_all = pd.read_pickle(os.path.join(PICKLE_DIR, 'session_info.pkl'))
            pilot_info_fr_all = pd.read_pickle(os.path.join(PICKLE_DIR, 'pilot_info.pkl'))
            unit_info_fr_all = pd.read_pickle(os.path.join(PICKLE_DIR, 'unit_info.pkl'))
            ses_prof_fr_all = pd.read_pickle(os.path.join(PICKLE_DIR, 'session_prof.pkl'))
            pilot_prof_fr_all = pd.read_pickle(os.path.join(PICKLE_DIR, 'pilot_prof.pkl'))
            cu_prof_fr_all = pd.read_pickle(os.path.join(PICKLE_DIR, 'unit_prof.pkl'))
            tr_cu_prof_fr_all = pd.read_pickle(os.path.join(PICKLE_DIR, 'tr_unit_prof.pkl'))
        except IOError:
            report.error("Pickle file not found!")
            exit(1)

    elif storage == "void":
        # Only for testing
        pass
    else:
        raise Exception("Unknown storage type")

    for sid in session_ids:

        if storage == "void":
            continue
        else:

            if not (len(ses_info_fr_all) and sid in ses_info_fr_all.index):
                report.warn("Session %s not in ses_info_fr_all.\n" % sid)
            else:
                report.warn("Removing session %s from ses_info_fr_all.\n" % sid)
                to_remove = ses_info_fr_all['name' == sid]
                print to_remove
                #ses_info_fr_all = ses_info_fr_all.append(ses_info_fr)
                pass

            if not ('sid' in pilot_info_fr_all and (pilot_info_fr_all['sid'] == sid).any()):
                report.warn("Session %s not in pilot_info_fr_all.\n" % sid)
            else:
                #pilot_info_fr_all = pilot_info_fr_all.append(pilot_info_fr)
                pass

            if not ('sid' in unit_info_fr_all and (unit_info_fr_all['sid'] == sid).any()):
                report.warn("Session %s not in unit_info_fr_all.\n" % sid)
            else:
                #unit_info_fr_all = unit_info_fr_all.append(unit_info_fr)
                pass

            if not ('sid' in ses_prof_fr_all and (ses_prof_fr_all['sid'] == sid).any()):
                report.warn("Session %s not in ses_prof_fr_all.\n" % sid)
            else:
                #ses_prof_fr_all = ses_prof_fr_all.append(ses_prof_fr)
                pass

            if not ('sid' in pilot_prof_fr_all and (pilot_prof_fr_all['sid'] == sid).any()):
                report.warn("Session %s not in pilot_prof_fr_all.\n" % sid)
            else:
                #pilot_prof_fr_all = pilot_prof_fr_all.append(pilot_prof_fr)
                pass

            if not ('sid' in cu_prof_fr_all and (cu_prof_fr_all['sid'] == sid).any()):
                report.warn("Session %s not in cu_prof_fr_all.\n" % sid)
            else:
                #cu_prof_fr_all = cu_prof_fr_all.append(cu_prof_fr)
                pass

            if not ('sid' in tr_cu_prof_fr_all and (tr_cu_prof_fr_all['sid'] == sid).any()):
                report.warn("Session %s not in tr_cu_prof_fr_all.\n" % sid)
            else:
                #tr_cu_prof_fr_all = tr_cu_prof_fr_all.append(tr_cu_prof_fr)
                pass

    # Write results back to disk
    # ses_info_fr_all.to_pickle(os.path.join(PICKLE_DIR, 'session_info.pkl'))
    # pilot_info_fr_all.to_pickle(os.path.join(PICKLE_DIR, 'pilot_info.pkl'))
    # unit_info_fr_all.to_pickle(os.path.join(PICKLE_DIR, 'unit_info.pkl'))
    # ses_prof_fr_all.to_pickle(os.path.join(PICKLE_DIR, 'session_prof.pkl'))
    # pilot_prof_fr_all.to_pickle(os.path.join(PICKLE_DIR, 'pilot_prof.pkl'))
    # cu_prof_fr_all.to_pickle(os.path.join(PICKLE_DIR, 'unit_prof.pkl'))
    # tr_cu_prof_fr_all.to_pickle(os.path.join(PICKLE_DIR, 'tr_unit_prof.pkl'))


###############################################################################
#
if __name__ == '__main__':

    report = ru.Reporter("Remove session from database.")

    session_ids = []

    # Read from file if specified, otherwise read from stdin
    f = open(sys.argv[1]) if len(sys.argv) > 1 else sys.stdin
    for line in f:
        session_ids.append(line.strip())

    if not session_ids:
        report.error("No sessions specified!")
        exit(1)

    remove_all(session_ids, 'pickle')
