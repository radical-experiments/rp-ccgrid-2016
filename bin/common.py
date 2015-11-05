import os

EXPERIMENT_DIR = '/Users/mark/ccgrid16/experiments'

CLIENT_DIR = os.path.join(EXPERIMENT_DIR, 'client')
TARGET_DIR = os.path.join(EXPERIMENT_DIR, 'data/profiling')
JSON_DIR = os.path.join(EXPERIMENT_DIR, 'data/json')
HDF5_DIR   = os.path.join(EXPERIMENT_DIR, 'data/hdf5')
PICKLE_DIR = os.path.join(EXPERIMENT_DIR, 'data/pickle')

RADICAL_PILOT_DBURL = None

###############################################################################
# Get the pilots ppn values for this session
def get_ppn(unit_info_df, pilot_info_df, sid):

    ppn_values = {}

    # Get all units and all pilots for session
    unit_info = unit_info_df[unit_info_df['sid'] == sid]
    pilot_info = pilot_info_df[pilot_info_df['sid'] == sid]

    pilots_in_session = unit_info['pilot'].unique()

    for pilot_id in pilots_in_session:
        pilot = pilot_info.loc[pilot_id]
        ppn = pilot['agent_config.cores_per_node']

        ppn_values[pilot_id] = ppn

    return ppn_values
