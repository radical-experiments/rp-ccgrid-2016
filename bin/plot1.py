import os
import sys
import time
import glob
import pandas as pd

EXPERIMENT_DIR = '/Users/mark/ccgrid16/experiments'

CLIENT_DIR = os.path.join(EXPERIMENT_DIR, 'bin')
TARGET_DIR = os.path.join(EXPERIMENT_DIR, 'data/profiling')
JSON_DIR   = os.path.join(EXPERIMENT_DIR, 'data/json')
HDF5_DIR   = os.path.join(EXPERIMENT_DIR, 'data/hdf5')
PICKLE_DIR = os.path.join(EXPERIMENT_DIR, 'data/pickle')

# Global Pandas settings
pd.set_option('display.width', 180)
pd.set_option('io.hdf.default_format','table')

import matplotlib as mp


###############################################################################
# Get the pilots resource labels for this session
def get_resources(unit_info_df, pilot_info_df, sid):

    resources = {}

    # Get all units and all pilots for session
    unit_info = unit_info_df[unit_info_df['sid'] == sid]
    pilot_info = pilot_info_df[pilot_info_df['sid'] == sid]

    pilots_in_session = unit_info['pilot'].unique()

    for pilot_id in pilots_in_session:
        pilot = pilot_info.loc[pilot_id]
        label = pilot['description.resource']

        resources[pilot_id] = label

    return resources


###############################################################################
# Get the pilots spawners for this session
def get_spawners(unit_info_df, pilot_info_df, sid):

    spawners = {}

    # Get all units and all pilots for session
    unit_info = unit_info_df[unit_info_df['sid'] == sid]
    pilot_info = pilot_info_df[pilot_info_df['sid'] == sid]

    pilots_in_session = unit_info['pilot'].unique()

    for pilot_id in pilots_in_session:
        pilot = pilot_info.loc[pilot_id]
        spawner = pilot['agent_config.spawner']

        spawners[pilot_id] = spawner

    return spawners


###############################################################################
# Get the pilots launch methods for this session
def get_lm(unit_info_df, pilot_info_df, sid, mpi):

    lms = {}

    # Get all units and all pilots for session
    unit_info = unit_info_df[unit_info_df['sid'] == sid]
    pilot_info = pilot_info_df[pilot_info_df['sid'] == sid]

    pilots_in_session = unit_info['pilot'].unique()

    for pilot_id in pilots_in_session:
        pilot = pilot_info.loc[pilot_id]
        if mpi:
            lm = pilot['agent_config.mpi_launch_method']
        else:
            lm = pilot['agent_config.task_launch_method']
        lms[pilot_id] = lm

    return lms

###############################################################################
# Get the value of MPI for the first CU (assuming all equal)
def get_mpi(unit_info_df, sid):

    # Get all units and all pilots for session
    unit_info = unit_info_df[unit_info_df['sid'] == sid]
    return unit_info.iloc[0]['description.mpi']


###############################################################################
#
# TODO: add concurrent CUs on right axis
def plot(tr_unit_prof_df, info_df, unit_info_df, pilot_info_df, sid):

    labels = []

    # Legend info
    info = info_df.loc[sid]

    mpi = get_mpi(unit_info_df, sid)
    #mpi = True
    # For this call assume that there is only one pilot per session
    lms = get_lm(unit_info_df, pilot_info_df, sid, mpi)
    assert len(lms) == 1
    launch_method = lms.values()[0]

    # For this call assume that there is only one pilot per session
    spawners = get_spawners(unit_info_df, pilot_info_df, sid)
    assert len(spawners) == 1
    spawner = spawners.values()[0]

    #exit()

    # For this call assume that there is only one pilot per session
    resources = get_resources(unit_info_df, pilot_info_df, sid)
    assert len(resources) == 1
    resource_label = resources.values()[0]

    # Get only the entries for this session
    tuf = tr_unit_prof_df[tr_unit_prof_df['sid'] == sid]

    tuf = tuf[tuf['Done'].notnull()]

    # We sort the units based on the order ...
    #tufs = tuf.sort('awo_get_u_pend') # they arrived at the agent
    #tufs = tuf.sort('aec_work_u_pend') # they are picked up by an EW
    tufs = tuf.sort('asc_put_u_pend') # they are scheduled
    #tufs = tuf.sort('asc_get_u_pend') # the are picked up by the scheduler

    ax = (tufs['asc_released'] - tufs['asc_allocated'] - info['metadata.cu_runtime']).plot(kind='line', color='red')
    labels.append("Core Occupation overhead")

    ax = (tufs['aec_after_exec'] - tufs['aec_after_cd'] - info['metadata.cu_runtime']).plot(kind='line', color='orange')
    labels.append('%s LaunchMethod (%s)' % ('MPI' if mpi else 'Task', launch_method))

    ax = (tufs['aec_start_script'] - tufs['aec_handover']).plot(kind='line', color='black')
    labels.append("Spawner (%s)" % spawner)

    (tufs['asc_get_u_pend'] - tufs['asic_put_u_pend']).plot(kind='line', color='blue')
    labels.append("Scheduler Queue")

    ax = (tufs['aec_work_u_pend'] - tufs['asc_put_u_pend']).plot(kind='line', color='green')
    labels.append("ExecWorker Queue")

    mp.pyplot.legend(labels, loc='upper left', fontsize=5)
    mp.pyplot.title("%s (%s)\n"
                    "%d CUs of %d core(s) with a %ds payload on a %d core pilot on %s.\n"
                    "%d sub-agent(s) with %d ExecWorker(s) each. All times are per CU.\n"
                    "RP: %s - RS: %s - RU: %s"
                   % (sid, time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime(info['created'])),
                      info['metadata.cu_count'], info['metadata.cu_cores'], info['metadata.cu_runtime'], info['metadata.pilot_cores'], resource_label,
                      info['metadata.num_sub_agents'], info['metadata.num_exec_instances_per_sub_agent'],
                      info['metadata.radical_stack.rp'], info['metadata.radical_stack.rs'], info['metadata.radical_stack.ru']
                      ), fontsize=8)
    mp.pyplot.xlabel("Compute Units (ordered by agent arrival)")
    mp.pyplot.ylabel("Time (s)")
    mp.pyplot.ylim(-0.01)
    ax.get_xaxis().set_ticks([])

    mp.pyplot.savefig('%s_plot1.pdf' % sid)
    mp.pyplot.close()

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

    session_ids = []

    # Read from file if specified, otherwise read from stdin
    f = open(sys.argv[1]) if len(sys.argv) > 1 else sys.stdin
    for line in f:
        session_ids.append(line.strip())

    if not session_ids:
        session_ids = find_sessions(JSON_DIR)

    unit_info_df = pd.read_pickle(os.path.join(PICKLE_DIR, 'unit_info.pkl'))
    pilot_info_df = pd.read_pickle(os.path.join(PICKLE_DIR, 'pilot_info.pkl'))
    tr_unit_prof_df = pd.read_pickle(os.path.join(PICKLE_DIR, 'tr_unit_prof.pkl'))
    session_info_df = pd.read_pickle(os.path.join(PICKLE_DIR, 'session_info.pkl'))

    for sid in session_ids:
        plot(tr_unit_prof_df, session_info_df, unit_info_df, pilot_info_df, sid)
