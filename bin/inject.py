import os
import sys
import glob
import radical.pilot.utils as rpu
import radical.utils as ru
import pandas as pd

EXPERIMENT_DIR = '/Users/mark/ipdps16/experiments'

CLIENT_DIR = os.path.join(EXPERIMENT_DIR, 'bin')
TARGET_DIR = os.path.join(EXPERIMENT_DIR, 'data/profiling')
JSON_DIR   = os.path.join(EXPERIMENT_DIR, 'data/json')
HDF5_DIR   = os.path.join(EXPERIMENT_DIR, 'data/hdf5')
PICKLE_DIR = os.path.join(EXPERIMENT_DIR, 'data/pickle')

RADICAL_PILOT_DBURL = None

# Global Pandas settings
pd.set_option('display.width', 300)
pd.set_option('io.hdf.default_format','table')

#
# Turn ID into a name that can be used as a python identifier.
#
def normalize_id(sid):
    return sid.replace('.', '_')

###############################################################################
# Convert from unicode to strings
def convert(input):
    if isinstance(input, dict):
        return {convert(key): convert(value) for key, value in input.iteritems()}
    elif isinstance(input, list):
        return [convert(element) for element in input]
    elif isinstance(input, unicode):
        return input.encode('utf-8')
    else:
        return input

###############################################################################
#
def json2frame(db, sid):

    json_docs = convert(rpu.get_session_docs(db, sid, cachedir=JSON_DIR))

    session_info = pd.io.json.json_normalize(json_docs['session'])
    session_info.set_index('_id', inplace=True)
    session_info.index.name = None

    unit_info = pd.io.json.json_normalize(json_docs['unit'])
    unit_info.set_index('_id', inplace=True)
    unit_info.index.name = None
    unit_info.insert(0, 'sid', sid)

    pilot_info = pd.io.json.json_normalize(json_docs['pilot'])
    pilot_info.set_index('_id', inplace=True)
    pilot_info.index.name = None
    pilot_info.insert(0, 'sid', sid)

    return session_info, pilot_info, unit_info

###############################################################################
#
def find_profiles(sid):

    session_dir = os.path.join(TARGET_DIR, sid)
    profile_paths = glob.glob('%s/*.prof' % session_dir)
    if not profile_paths:
        raise Exception("No session files found in directory %s" % session_dir)

    print "Found profiling files in %s: %s" % (session_dir, profile_paths)

    return profile_paths

###############################################################################
#
def inject(sid):

    #norm_sid = normalize_id(sid)
    norm_sid = sid

    sid_profiles = find_profiles(sid)
    print sid_profiles
    report.info("Combining profiles for session: %s.\n" % sid)
    combined_profiles = rpu.combine_profiles(sid_profiles)
    uids = set()
    for p in combined_profiles:
        uids.add(p['uid'])

    print uids
    #exit()

    report.info("Converting profiles to frames for session: %s.\n" % sid)
    frames = rpu.prof2frame(combined_profiles)

    report.info("Head of Combined DF for session %s:\n" % sid)
    #print frames.head()
    print frames.entity.unique()

    ses_prof_fr, pilot_prof_fr, cu_prof_fr = rpu.split_frame(frames)

    report.info("Head of Session DF for session %s:\n" % sid)
    ses_prof_fr.insert(0, 'sid', norm_sid)
    print ses_prof_fr.head()

    report.info("Head of Pilot DF for session %s:\n" % sid)
    pilot_prof_fr.insert(0, 'sid', norm_sid)
    print pilot_prof_fr.head()

    report.info("Head of CU DF for session %s:\n" % sid)
    rpu.add_states(cu_prof_fr)
    rpu.add_info(cu_prof_fr)

    cu_prof_fr.insert(0, 'sid', norm_sid)
    print cu_prof_fr.head()

    # transpose
    tr_cu_prof_fr = rpu.get_info_df(cu_prof_fr)
    tr_cu_prof_fr.insert(0, 'sid', norm_sid)
    report.info("Head of Transposed CU DF for session %s:\n" % sid)
    print tr_cu_prof_fr.head()

    ses_info_fr, pilot_info_fr, unit_info_fr = json2frame(db=None, sid=sid)
    report.info("Head of json Docs for session %s:\n" % sid)
    print ses_info_fr.head()

    return ses_info_fr, pilot_info_fr, unit_info_fr, ses_prof_fr, \
           pilot_prof_fr, cu_prof_fr, tr_cu_prof_fr


###############################################################################
#
def inject_all(session_ids, storage):

    ses_info_fr_all = pd.DataFrame()
    pilot_info_fr_all = pd.DataFrame()
    unit_info_fr_all = pd.DataFrame()
    ses_prof_fr_all = pd.DataFrame()
    pilot_prof_fr_all = pd.DataFrame()
    cu_prof_fr_all = pd.DataFrame()
    tr_cu_prof_fr_all = pd.DataFrame()

    if storage == 'hdf5':
        store = pd.HDFStore(os.path.join(HDF5_DIR, 'store.h5'))
    elif storage == 'pickle':
        pass
    elif storage == "void":
        pass
    else:
        raise Exception("Unknown storage type")

    for sid in session_ids:

        norm_sid = normalize_id(sid)

        if storage == "void":
            inject(sid)
            continue
        else:
            ses_info_fr, pilot_info_fr, unit_info_fr, \
            ses_prof_fr, pilot_prof_fr, cu_prof_fr, tr_cu_prof_fr = \
                    inject(sid)


        ses_info_fr_all = ses_info_fr_all.append(ses_info_fr)
        pilot_info_fr_all = pilot_info_fr_all.append(pilot_info_fr)
        unit_info_fr_all = unit_info_fr_all.append(unit_info_fr)
        ses_prof_fr_all = ses_prof_fr_all.append(ses_prof_fr)
        pilot_prof_fr_all = pilot_prof_fr_all.append(pilot_prof_fr)
        cu_prof_fr_all = cu_prof_fr_all.append(cu_prof_fr)
        tr_cu_prof_fr_all = tr_cu_prof_fr_all.append(tr_cu_prof_fr)

    if storage == 'hdf5':

        store.append('session_info', ses_info_fr_all)
        store.append('pilot_info', pilot_info_fr_all.convert_objects())
        store.append('unit_info', unit_info_fr_all)
        store.append('session_prof', ses_prof_fr_all)
        store.append('pilot_prof', pilot_prof_fr_all)
        store.append('unit_prof', cu_prof_fr_all)
        store.append('tr_unit_prof', tr_cu_prof_fr_all.convert_objects())

        store.close()

    elif storage == 'pickle':

        ses_info_fr_all.to_pickle(os.path.join(PICKLE_DIR, 'session_info.pkl'))
        pilot_info_fr_all.to_pickle(os.path.join(PICKLE_DIR, 'pilot_info.pkl'))
        unit_info_fr_all.to_pickle(os.path.join(PICKLE_DIR, 'unit_info.pkl'))
        ses_prof_fr_all.to_pickle(os.path.join(PICKLE_DIR, 'session_prof.pkl'))
        pilot_prof_fr_all.to_pickle(os.path.join(PICKLE_DIR, 'pilot_prof.pkl'))
        cu_prof_fr_all.to_pickle(os.path.join(PICKLE_DIR, 'unit_prof.pkl'))
        tr_cu_prof_fr_all.to_pickle(os.path.join(PICKLE_DIR, 'tr_unit_prof.pkl'))

    elif storage == "void":
        pass

###############################################################################
#
def find_sessions(json_dir):

    session_paths = glob.glob('%s/rp.session.*json' % json_dir)
    if not session_paths:
        raise Exception("No session files found in directory %s" % json_dir)

    session_files = [os.path.basename(e) for e in session_paths]

    session_ids = [e.rsplit('.json')[0] for e in session_files]

    print "Found sessions in %s: %s" % (json_dir, session_ids)

    return session_ids


###############################################################################
#
if __name__ == '__main__':

    report = ru.Reporter("Inject profiling and json data into database.")

    session_ids = []

    # Read from file if specified, otherwise read from stdin
    f = open(sys.argv[1]) if len(sys.argv) > 1 else sys.stdin
    for line in f:
        session_ids.append(line.strip())

    if not session_ids:
        session_ids = find_sessions(JSON_DIR)

    inject_all(session_ids, 'pickle')
