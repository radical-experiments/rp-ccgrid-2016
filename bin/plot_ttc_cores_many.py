import os
import sys
import time
import glob
import pandas as pd

from common import PICKLE_DIR, get_resources

# Global Pandas settings
pd.set_option('display.width', 180)
pd.set_option('io.hdf.default_format','table')

import matplotlib as mp

colors = { 'pilot_bar': 'green', 'gen_bar': 'red', 'client_bar': 'blue'}

###############################################################################
#
# TODO: add concurrent CUs on right axis
def plot(tr_unit_prof_df, info_df, unit_info_df, pilot_info_df, sids):

    labels = []


    for key in sids:

        orte_ttc = {}

        for sid in sids[key]:

            # Legend info
            info = info_df.loc[sid]

            cores = info['metadata.cu_count'] # / info['metadata.generations']

            if key == 'pilot_bar':
                cores /= 5

            if cores not in orte_ttc:
                orte_ttc[cores] = []

            # For this call assume that there is only one pilot per session
            resources = get_resources(unit_info_df, pilot_info_df, sid)
            assert len(resources) == 1
            resource_label = resources.values()[0]

            # Get only the entries for this session
            tuf = tr_unit_prof_df[tr_unit_prof_df['sid'] == sid]

            # Only take completed CUs into account
            tuf = tuf[tuf['Done'].notnull()]

            # We sort the units based on the order they arrived at the agent
            tufs = tuf.sort('awo_get_u_pend')

            orte_ttc[cores].append((tufs['aec_after_exec'].max() - tufs['awo_get_u_pend'].min()))


        orte_df = pd.DataFrame(orte_ttc)

        labels.append("Config: %s" % key)
        ax = orte_df.mean().plot(kind='line', color=colors[key])

    print 'labels: %s' % labels
    mp.pyplot.legend(labels, loc='upper left', fontsize=5)
    mp.pyplot.title("TTC for a varying number of 'concurrent' CUs.\n"
                    "%d generations of a variable number of 'concurrent' CUs of %d core(s) with a %ss payload on a variable core pilot on %s.\n"
                    "Constant number of %d sub-agent with %d ExecWorker(s) each.\n"
                    "RP: %s - RS: %s - RU: %s"
                   % (info['metadata.generations'], info['metadata.cu_cores'], info['metadata.cu_runtime'], resource_label,
                      info['metadata.num_sub_agents'], info['metadata.num_exec_instances_per_sub_agent'],
                      info['metadata.radical_stack.rp'], info['metadata.radical_stack.rs'], info['metadata.radical_stack.ru']
                      ), fontsize=8)
    mp.pyplot.xlabel("Number of Cores")
    mp.pyplot.ylabel("Time to Completion (s)")
    #mp.pyplot.ylim(0)
    #ax.get_xaxis().set_ticks([])
    #ax.get_xaxis.set

    mp.pyplot.savefig('plot_ttc_cores_many.pdf')
    mp.pyplot.close()


###############################################################################
#
if __name__ == '__main__':

    unit_info_df = pd.read_pickle(os.path.join(PICKLE_DIR, 'unit_info.pkl'))
    pilot_info_df = pd.read_pickle(os.path.join(PICKLE_DIR, 'pilot_info.pkl'))
    tr_unit_prof_df = pd.read_pickle(os.path.join(PICKLE_DIR, 'tr_unit_prof.pkl'))
    session_info_df = pd.read_pickle(os.path.join(PICKLE_DIR, 'session_info.pkl'))

    session_ids = {
        'pilot_bar': [
            # Comet after scheduler fix2
            "rp.session.ip-10-184-31-85.santcroos.016747.0011", # 1 node
            "rp.session.ip-10-184-31-85.santcroos.016747.0009", # 2 node
            "rp.session.ip-10-184-31-85.santcroos.016747.0008", # 4 node
            "rp.session.ip-10-184-31-85.santcroos.016747.0010", # 8 nodes
            "rp.session.ip-10-184-31-85.santcroos.016747.0013", # 16 nodes
            "rp.session.ip-10-184-31-85.santcroos.016747.0000", # 32 nodes
            "rp.session.ip-10-184-31-85.santcroos.016747.0001", # 48 nodes
        ],
        'gen_bar': [
            # Comet generation barrier
            "rp.session.ip-10-184-31-85.santcroos.016758.0016", # 1
            "rp.session.ip-10-184-31-85.santcroos.016758.0009", # 2
            "rp.session.ip-10-184-31-85.santcroos.016758.0015", # 4
            "rp.session.ip-10-184-31-85.santcroos.016758.0010", # 8
            "rp.session.ip-10-184-31-85.santcroos.016758.0019", # 16
            "rp.session.ip-10-184-31-85.santcroos.016758.0000", # 32
            "rp.session.ip-10-184-31-85.santcroos.016758.0020", # 48
        ],
        'client_bar': [
            "rp.session.ip-10-184-31-85.santcroos.016759.0016", # 1
            "rp.session.ip-10-184-31-85.santcroos.016759.0015", # 2
            "rp.session.ip-10-184-31-85.santcroos.016759.0014", # 4
            "rp.session.ip-10-184-31-85.santcroos.016759.0009", # 8
            "rp.session.ip-10-184-31-85.santcroos.016759.0001", # 16
            "rp.session.ip-10-184-31-85.santcroos.016759.0000", # 32
            "rp.session.ip-10-184-31-85.santcroos.016759.0010", # 48
            "rp.session.ip-10-184-31-85.santcroos.016760.0002",
        ]
}

    plot(tr_unit_prof_df, session_info_df, unit_info_df, pilot_info_df, session_ids)
