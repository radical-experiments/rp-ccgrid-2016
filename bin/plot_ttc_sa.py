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


###############################################################################
#
# TODO: add concurrent CUs on right axis
def plot(tr_unit_prof_df, info_df, unit_info_df, pilot_info_df, sids):

    labels = []

    orte_ttc = {}

    for sid in sids:


        # Legend info
        info = info_df.loc[sid]

        num_sas = info['metadata.num_sub_agents']

        if num_sas not in orte_ttc:
            orte_ttc[num_sas] = []

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

        orte_ttc[num_sas].append((tufs['aec_after_exec'].max() - tufs['awo_get_u_pend'].min()))

    orte_df = pd.DataFrame(orte_ttc)

    ax = orte_df.mean().plot(kind='bar', colormap='Paired')

    mp.pyplot.title("TTC for varying Sub-Agents.\n"
                    "%d CUs of %d core(s) with a %ss payload on a %d core pilot on %s.\n"
                    "Varying amount of sub-agents with single ExecWorker.\n"
                    "RP: %s - RS: %s - RU: %s"
                   % (info['metadata.cu_count'], info['metadata.cu_cores'], info['metadata.cu_runtime'], info['metadata.pilot_cores'], resource_label,
                      info['metadata.radical_stack.rp'], info['metadata.radical_stack.rs'], info['metadata.radical_stack.ru']
                      ), fontsize=8)
    mp.pyplot.xlabel("Number of Sub-Agents")
    mp.pyplot.ylabel("Time to Completion (s)")
    #mp.pyplot.ylim(0)
    #ax.get_xaxis().set_ticks([])
    #ax.get_xaxis.set

    mp.pyplot.savefig('plot_ttc_sa.pdf')
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

    unit_info_df = pd.read_pickle(os.path.join(PICKLE_DIR, 'unit_info.pkl'))
    pilot_info_df = pd.read_pickle(os.path.join(PICKLE_DIR, 'pilot_info.pkl'))
    tr_unit_prof_df = pd.read_pickle(os.path.join(PICKLE_DIR, 'tr_unit_prof.pkl'))
    session_info_df = pd.read_pickle(os.path.join(PICKLE_DIR, 'session_info.pkl'))

    session_ids = [
        "rp.session.netbook.mark.016741.0006", # 1
        "rp.session.netbook.mark.016740.0016", # 2
        "rp.session.netbook.mark.016740.0015", # 4
        "rp.session.netbook.mark.016740.0012", # 8
        #"rp.session.netbook.mark.016740.0017", # 16
        "rp.session.netbook.mark.016740.0013", # 32
    ]

    plot(tr_unit_prof_df, session_info_df, unit_info_df, pilot_info_df, session_ids)
