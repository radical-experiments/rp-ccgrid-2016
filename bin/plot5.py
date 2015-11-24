import os
import sys
import time
import glob
import pandas as pd

from common import PICKLE_DIR, get_ppn, get_resources

# Global Pandas settings
pd.set_option('display.width', 180)
pd.set_option('io.hdf.default_format','table')

import matplotlib as mp


###############################################################################
#
# TODO: add concurrent CUs on right axis
def plot(sids, value, label=''):

    labels = []

    first = True

    for sid in sids:

        session_dir = os.path.join(PICKLE_DIR, sid)

        unit_info_df = pd.read_pickle(os.path.join(session_dir, 'unit_info.pkl'))
        pilot_info_df = pd.read_pickle(os.path.join(session_dir, 'pilot_info.pkl'))
        unit_prof_df = pd.read_pickle(os.path.join(session_dir, 'unit_prof.pkl'))
        session_info_df = pd.read_pickle(os.path.join(session_dir, 'session_info.pkl'))

        # Legend info
        info = session_info_df.loc[sid]

        # For this call assume that there is only one pilot per session
        resources = get_resources(unit_info_df, pilot_info_df, sid)
        assert len(resources) == 1
        resource_label = resources.values()[0]

        # Get only the entries for this session
        #uf = unit_prof_df[unit_prof_df['sid'] == sid]

        # We sort the units based on the order they arrived at the agent
        #ufs = uf.sort('awo_get_u_pend')

        cores = info['metadata.effective_cores']

        if value == 'sched':
            #
            # Scheduling
            #
            df = unit_prof_df[
                (unit_prof_df.cc_sched >= 0) &
                (unit_prof_df.event == 'advance') &
                (unit_prof_df.sid == sid)
                ][['time', 'cc_sched']]

        elif value == 'exec':
            #
            # Scheduling
            #
            df = unit_prof_df[
                (unit_prof_df.cc_exec >= 0) &
                (unit_prof_df.event == 'advance') &
                (unit_prof_df.sid == sid)
                ][['time', 'cc_exec']]

        else:
            raise Exception("Value %s unknown" % value)

        df.columns = ['time', cores]
        df['time'] -= df['time'].min()

        if first:
            df_all = df
        else:
            df_all = pd.merge(df_all, df,  on='time', how='outer')

        labels.append("Cores: %d" % cores)

        first = False

    df_all.set_index('time', inplace=True)
    print df_all.head()
    #df_all.plot(colormap='Paired')
    df_all.plot()

    # For this call assume that there is only one pilot per session
    ppn_values = get_ppn(unit_info_df, pilot_info_df, sid)
    assert len(ppn_values) == 1
    ppn = ppn_values.values()[0]

    mp.pyplot.legend(labels, loc='upper right', fontsize=5)
    mp.pyplot.title("Concurrent number of CUs in stage '%s'.\n"
                    "%d generations of a variable number of 'concurrent' CUs of %d core(s) with a %ss payload on a variable core pilot on %s.\n"
                    "Constant number of %d sub-agent with %d ExecWorker(s) each.\n"
                    "RP: %s - RS: %s - RU: %s"
                   % (value,
                      info['metadata.generations'], info['metadata.cu_cores'], info['metadata.cu_runtime'], resource_label,
                      info['metadata.num_sub_agents'], info['metadata.num_exec_instances_per_sub_agent'],
                      info['metadata.radical_stack.rp'], info['metadata.radical_stack.rs'], info['metadata.radical_stack.ru']
                      ), fontsize=8)
    mp.pyplot.xlabel("Time (s)")
    mp.pyplot.ylabel("Concurrent Compute Units")
    #mp.pyplot.ylim(0)
    #mp.pyplot.xlim(0, 1800)
    #ax.get_xaxis().set_ticks([])

    mp.pyplot.savefig('plot5_%s%s.pdf' % (value, label))
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

    session_ids = [
        # Comet before scheduler fix:
        # "rp.session.ip-10-184-31-85.santcroos.016743.0005",
        # "rp.session.ip-10-184-31-85.santcroos.016743.0007",
        # "rp.session.ip-10-184-31-85.santcroos.016743.0002",
        # "rp.session.ip-10-184-31-85.santcroos.016743.0008",
        # "rp.session.ip-10-184-31-85.santcroos.016743.0003",
        # "rp.session.ip-10-184-31-85.santcroos.016743.0006",
        # "rp.session.ip-10-184-31-85.santcroos.016743.0001",
        # Comet after scheduler fix:
        #"rp.session.ip-10-184-31-85.santcroos.016744.0000",
        #"rp.session.ip-10-184-31-85.santcroos.016744.0001",
        #"rp.session.ip-10-184-31-85.santcroos.016744.0002",
        #"rp.session.ip-10-184-31-85.santcroos.016744.0003",
        #"rp.session.ip-10-184-31-85.santcroos.016744.0004",
        #"rp.session.ip-10-184-31-85.santcroos.016745.0000",
        #"rp.session.ip-10-184-31-85.santcroos.016745.0001",
        #"rp.session.ip-10-184-31-85.santcroos.016745.0004",
        #

        # Comet after scheduler fix 2:
        "rp.session.ip-10-184-31-85.santcroos.016747.0011", # 1 node
        "rp.session.ip-10-184-31-85.santcroos.016747.0009", # 2 node
        "rp.session.ip-10-184-31-85.santcroos.016747.0008", # 4 node
        "rp.session.ip-10-184-31-85.santcroos.016747.0010", # 8 nodes
        "rp.session.ip-10-184-31-85.santcroos.016747.0013", # 16 nodes
        "rp.session.ip-10-184-31-85.santcroos.016747.0000", # 32 nodes
        "rp.session.ip-10-184-31-85.santcroos.016747.0001", # 48 nodes

        # Titan after scheduler fix
        # "rp.session.titan-ext7.marksant1.016747.0002", # 16
        # "rp.session.titan-ext7.marksant1.016747.0008", # 32
        # "rp.session.titan-ext7.marksant1.016747.0005", # 64
        # "rp.session.titan-ext7.marksant1.016747.0007", # 128
        # "rp.session.titan-ext7.marksant1.016747.0003", # 256
        # "rp.session.titan-ext7.marksant1.016747.0006", # 512 cores / 32 nodes
        # "rp.session.titan-ext7.marksant1.016747.0009", # 1024 cores / 64 nodes
        # "rp.session.titan-ext7.marksant1.016747.0010", # 2048 cores / 128 nodes
    ]

    label = '_10sa_1ew'

    for value in ['sched', 'exec']:
        plot(session_ids, value, label)
