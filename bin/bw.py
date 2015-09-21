#!/usr/bin/env python

__copyright__ = "Copyright 2015, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import time
import os
import radical.pilot as rp
import random

# "Global" configs

# agent_config = {
#     "number_of_workers": {
#         "StageinWorker"   : 1,
#         "ExecWorker"      : 1,
#         "StageoutWorker"  : 1,
#         "UpdateWorker"    : 1
#     },
#     "blowup_factor": {
#         "Agent"           : 1,
#         "stagein_queue"   : 1,
#         "StageinWorker"   : 1,
#         "schedule_queue"  : 1,
#         "Scheduler"       : 1,
#         "execution_queue" : 1,
#         "ExecWorker"      : 1,
#         "watch_queue"     : 1,
#         "ExecWatcher"     : 1,
#         "stageout_queue"  : 1,
#         "StageoutWorker"  : 1,
#         "update_queue"    : 1,
#         "UpdateWorker"    : 1
#     },
#     "drop_clones": {
#         "Agent"           : 1,
#         "stagein_queue"   : 1,
#         "StageinWorker"   : 1,
#         "schedule_queue"  : 1,
#         "Scheduler"       : 1,
#         "execution_queue" : 1,
#         "ExecWorker"      : 1,
#         "watch_queue"     : 1,
#         "ExecWatcher"     : 1,
#         "stageout_queue"  : 1,
#         "StageoutWorker"  : 1,
#         "update_queue"    : 1,
#         "UpdateWorker"    : 1
#     }
# }

# Whether and how to install new RP remotely
RP_VERSION = "local" # debug, installed, local
VIRTENV_MODE = "create" # create, use, update

# Schedule CUs directly to a Pilot, assumes single Pilot
SCHEDULER = rp.SCHED_DIRECT_SUBMISSION

resource_config = {
    #
    # XE nodes have 2 "Interlagos" Processors with 8 "Bulldozer" cores each.
    # Every "Bulldozer" core consists of 2 schedualable integer cores.
    # XE nodes therefore have a PPN=32.
    #
    # Depending on the type of application,
    # one generally chooses to have 16 or 32 instances per node.

    'LOCAL': {
        'RESOURCE': 'local.localhost',
        'LAUNCH_METHOD': 'FORK',
        #'AGENT_SPAWNER': 'POPEN',
        'AGENT_SPAWNER': 'SHELL',
        'PPN': 4
    },
    'APRUN': {
        'RESOURCE': 'ncsa.bw',
        'LAUNCH_METHOD': 'APRUN',
        'AGENT_SPAWNER': 'SHELL',
        #'AGENT_SPAWNER': 'POPEN',
        'QUEUE': 'debug', # Maximum 30 minutes
        'PPN': 32
    },
    'CCM': {
        'RESOURCE': 'ncsa.bw_ccm',
        #'LAUNCH_METHOD': 'SSH',
        'LAUNCH_METHOD': 'MPIRUN',
        #'AGENT_SPAWNER': 'SHELL',
        'AGENT_SPAWNER': 'POPEN',
        'QUEUE': 'debug', # Maximum 30 minutes
        'PPN': 32,
        'PRE_EXEC_PREPEND': [
            'module use --append /u/sciteam/marksant/privatemodules',
            'module load use.own',
            'module load openmpi/1.8.4_ccm'
        ]
    },
    'ORTE': {
        'RESOURCE': 'ncsa.bw',
        'LAUNCH_METHOD': "ORTE",
        'AGENT_SPAWNER': 'SHELL',
    #nodes_var = [1]
        #'AGENT_SPAWNER': 'POPEN',
        #'QUEUE': 'debug', # Maximum 30 minutes
        'PPN': 32,
        'PRE_EXEC_PREPEND': [
            'module use --append /u/sciteam/marksant/privatemodules',
            'module load use.own',
            'module load openmpi/git'
        ]
    },
    'TITAN': {
        'RESOURCE': 'ornl.titan',
        'SCHEMA': 'local',
        'LAUNCH_METHOD': "ORTE",
        #'AGENT_SPAWNER': 'SHELL',
        'AGENT_SPAWNER': 'POPEN',
        #'QUEUE': 'debug', # Maximum 60 minutes
        'PROJECT': 'csc168',
        'PPN': 16,
        'PRE_EXEC_PREPEND': [
            #'module use --append /u/sciteam/marksant/privatemodules',
            #'module load use.own',
            #'module load openmpi/git'
        ]
    },
    'STAMPEDE': {
        'RESOURCE': 'xsede.stampede',
        #'SCHEMA': 'local',
        #'LAUNCH_METHOD': "ORTE",
        #'AGENT_SPAWNER': 'SHELL',
        #'AGENT_SPAWNER': 'POPEN',
        #'QUEUE': 'debug', # Maximum 30 minutes
        'PROJECT': 'TG-MCB090174',
        'PPN': 16,
        'PRE_EXEC_PREPEND': [
            #'module use --append /u/sciteam/marksant/privatemodules',
            #'module load use.own',
            #'module load openmpi/git'
        ]
    }
}



#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state):

    if not pilot:
        return

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if state == rp.FAILED:
        sys.exit (1)


CNT = 0
#------------------------------------------------------------------------------
#
def unit_state_cb (unit, state):

    if not unit:
        return

    global CNT

    print "[Callback]: unit %s on %s: %s." % (unit.uid, unit.pilot_id, state)

    if state in [rp.FAILED, rp.DONE, rp.CANCELED]:
        CNT += 1
        print "[Callback]: # %6d" % CNT


    if state == rp.FAILED:
        print "stderr: %s" % unit.stderr
        #sys.exit(2)


#------------------------------------------------------------------------------
#
def wait_queue_size_cb(umgr, wait_queue_size):
    print "[Callback]: wait_queue_size: %s." % wait_queue_size
#------------------------------------------------------------------------------

#--------------------------------------------------------------------------
#
def insert_exp_details(session, id):

    if session is None:
        raise Exception("No active session.")

    result = session._dbs._s.update(
        {"_id": session._uid},
        {"$set" : {"experiment": {'id': id}}}
    )

    # return the object id as a string
    return str(result)

#------------------------------------------------------------------------------
#
def run_experiment(backend, pilot_cores, pilot_runtime, cu_runtime, cu_cores, cu_count, profiling, agent_config):

    # Profiling
    if profiling:
        os.environ['RADICAL_PILOT_PROFILE'] = 'TRUE'

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()
    print "session id: %s" % session.uid
    print "Experiment - Backend:%s, PilotCores:%d, PilotRuntime:%d, CURuntime:%d, CUCores:%d, CUCount:%d" % \
        (backend, pilot_cores, pilot_runtime, cu_runtime, cu_cores, cu_count)

    cfg = session.get_resource_config(resource_config[backend]['RESOURCE'])

    # create a new config based on the old one, and set a different queue
    new_cfg = rp.ResourceConfig(resource_config[backend]['RESOURCE'], cfg)

    # Insert pre_execs at the beginning in reverse order
    if 'PRE_EXEC_PREPEND' in resource_config[backend]:
        for entry in resource_config[backend]['PRE_EXEC_PREPEND'][::-1]:
            new_cfg.pre_bootstrap.insert(0, entry)

    # Change launch method
    if 'LAUNCH_METHOD' in resource_config[backend]:
        new_cfg.task_launch_method = resource_config[backend]['LAUNCH_METHOD']

    # Change method to spawn tasks
    if 'AGENT_SPAWNER' in resource_config[backend]:
        new_cfg.agent_spawner = resource_config[backend]['AGENT_SPAWNER']

    # Don't install a new version of RP
    new_cfg.rp_version = RP_VERSION
    new_cfg.virtenv_mode = VIRTENV_MODE

    # now add the entry back.  As we did not change the config name, this will
    # replace the original configuration.  A completely new configuration would
    # need a unique name.
    session.add_resource_config(new_cfg)

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        pmgr = rp.PilotManager(session=session)
        pmgr.register_callback(pilot_state_cb)

        pdesc = rp.ComputePilotDescription()
        pdesc.resource = resource_config[backend]['RESOURCE']
        pdesc.cores = pilot_cores
        if 'QUEUE' in resource_config[backend]:
            pdesc.queue = resource_config[backend]['QUEUE']
        if 'SCHEMA' in resource_config[backend]:
            pdesc.access_schema = resource_config[backend]['SCHEMA']
        if 'PROJECT' in resource_config[backend]:
            pdesc.project = resource_config[backend]['PROJECT']
        pdesc.runtime = pilot_runtime
        pdesc.cleanup = False

        pdesc._config = agent_config

        pilot = pmgr.submit_pilots(pdesc)

        umgr = rp.UnitManager(session=session, scheduler=SCHEDULER)
        umgr.register_callback(unit_state_cb, rp.UNIT_STATE)
        umgr.register_callback(wait_queue_size_cb, rp.WAIT_QUEUE_SIZE)
        umgr.add_pilots(pilot)

        cuds = list()
        for unit_count in range(0, cu_count):
            cud = rp.ComputeUnitDescription()
            cud.executable     = "/bin/bash"
            cud.arguments      = ["-c", "date && hostname -f && sleep %d && date" % cu_runtime]
            cud.cores          = cu_cores
            cuds.append(cud)

        units = umgr.submit_units(cuds)
        umgr.wait_units()

        for cu in units:
            print "* Task %s state %s, exit code: %s, started: %s, finished: %s" \
                % (cu.uid, cu.state, cu.exit_code, cu.start_time, cu.stop_time)

    except Exception as e:
        # Something unexpected happened in the pilot code above
        print "caught Exception: %s" % e
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        print "need to exit now: %s" % e

    finally:
        # Give the agent chance to write all.
        time.sleep(5)

        print "inserting meta data into session"
        insert_exp_details(session, 42)

        print "closing session"
        session.close(cleanup=False, terminate=True)

        return session._uid
#
#------------------------------------------------------------------------------



#------------------------------------------------------------------------------
#
# Variable CU duration (0, 1, 10, 30, 60, 120)
# Fixed backend (ORTE)
# Fixed CU count (1024)
# Fixed CU cores (1)
# CU = /bin/sleep
# Fixed Pilot cores (256)
#
# Goal: investigate the relative overhead of ORTE in relation to the runtime of the CU
#
def exp1(repeat):

    agent_config = {}
    agent_config['number_of_workers'] = {}
    agent_config['number_of_workers']['ExecWorker'] = 1

    sessions = {}

    # Enable/Disable profiling
    profiling=True

    backend = 'ORTE'

    cu_cores = 1

    # The number of cores to acquire on the resource
    nodes = 8
    pilot_cores = int(resource_config[backend]['PPN']) * nodes

    # Maximum walltime for experiment
    pilot_runtime = 30 # should we guesstimate this?

    cu_count = 512

    for iter in range(repeat):

        runtimes = [0, 1, 10, 30, 60, 120, 300, 600]
        random.shuffle(runtimes)
        for cu_sleep in runtimes:

            sid = run_experiment(
                backend=backend,
                pilot_cores=pilot_cores,
                pilot_runtime=pilot_runtime,
                cu_runtime=cu_sleep,
                cu_cores=cu_cores,
                cu_count=cu_count,
                profiling=profiling,
                agent_config=agent_config
            )

            sessions[sid] = {
                'backend': backend,
                'pilot_cores': pilot_cores,
                'pilot_runtime': pilot_runtime,
                'cu_runtime': cu_sleep,
                'cu_cores': cu_cores,
                'cu_count': cu_count,
                'profiling': profiling,
                'iteration': iter,
                'number_of_workers': agent_config['number_of_workers']['ExecWorker']
        }

    return sessions
#
#-------------------------------------------------------------------------------


#------------------------------------------------------------------------------
#
# Fixed CU duration (60)
# Fixed backend (ORTE)
# Variable CU count (4-1024)
# Variable CU cores (1-256)
# CU = /bin/sleep
# Fixed Pilot cores (256)
#
# Goal: Investigate the relative overhead of small tasks compared to larger tasks
#
def exp2(repeat):

    agent_config = {}
    agent_config['number_of_workers'] = {}
    agent_config['number_of_workers']['ExecWorker'] = 1

    sessions = {}

    # Enable/Disable profiling
    profiling=True

    backend = 'ORTE'

    cu_sleep = 60

    # The number of cores to acquire on the resource
    nodes = 8
    pilot_cores = int(resource_config[backend]['PPN']) * nodes

    # Maximum walltime for experiment
    pilot_runtime = 30 # should we guesstimate this?

    for iter in range(repeat):

        cu_cores_var = [1, 2, 4, 8, 16, 32, 64, 128, 256]
        random.shuffle(cu_cores_var)
        for cu_cores in cu_cores_var:

            # keep core consumption equal (4 generations)
            cu_count = (4 * pilot_cores) / cu_cores

            sid = run_experiment(
                backend=backend,
                pilot_cores=pilot_cores,
                pilot_runtime=pilot_runtime,
                cu_runtime=cu_sleep,
                cu_cores=cu_cores,
                cu_count=cu_count,
                profiling=profiling,
                agent_config=agent_config
            )

            sessions[sid] = {
                'backend': backend,
                'pilot_cores': pilot_cores,
                'pilot_runtime': pilot_runtime,
                'cu_runtime': cu_sleep,
                'cu_cores': cu_cores,
                'cu_count': cu_count,
                'profiling': profiling,
                'iteration': iter,
                'number_of_workers': agent_config['number_of_workers']['ExecWorker']
            }

    return sessions
#
#-------------------------------------------------------------------------------

#------------------------------------------------------------------------------
#
# Fixed CU duration (0s)
# Fixed backend (ORTE)
# Fixed CU count (500)
# Fixed CU cores (1)
# CU = /bin/sleep
# Fixed Pilot cores (256)
# Variable number of exec workers (1-8)
#
# Goal: Investigate the effect of number of exec workers
#
def exp3(repeat):

    agent_config = {}
    agent_config['number_of_workers'] = {}

    sessions = {}

    # Enable/Disable profiling
    profiling=True

    backend = 'TITAN'

    cu_cores = 1

    # The number of cores to acquire on the resource
    nodes = 8
    pilot_cores = int(resource_config[backend]['PPN']) * nodes

    # Maximum walltime for experiment
    pilot_runtime = 30 # should we guesstimate this?

    cu_count = 512
    cu_sleep = 0

    for iter in range(repeat):

        workers_range = range(1,9)
        random.shuffle(workers_range)
        for workers in workers_range:
            agent_config['number_of_workers']['ExecWorker'] = workers

            sid = run_experiment(
                backend=backend,
                pilot_cores=pilot_cores,
                pilot_runtime=pilot_runtime,
                cu_runtime=cu_sleep,
                cu_cores=cu_cores,
                cu_count=cu_count,
                profiling=profiling,
                agent_config=agent_config
            )

            sessions[sid] = {
                'backend': backend,
                'pilot_cores': pilot_cores,
                'pilot_runtime': pilot_runtime,
                'cu_runtime': cu_sleep,
                'cu_cores': cu_cores,
                'cu_count': cu_count,
                'profiling': profiling,
                'iteration': iter,
                'number_of_workers': agent_config['number_of_workers']['ExecWorker']
            }

    return sessions
#
#-------------------------------------------------------------------------------

#------------------------------------------------------------------------------
#
# Fixed CU duration (60)
# Fixed backend (ORTE)
# Variable CU count (5 generations)
# Variable CU cores (1, 32)
# CU = /bin/sleep
# Variable Pilot cores (256, 512, 1024, 2048, 4096, 8192)
#
# Goals: A) Investigate the scale of things. 
#        B) Investigate the effect of 1 per node vs 32 per node
#
def exp4(repeat):

    f = open('exp4.txt', 'a')
    f.write('%s\n' % time.ctime())

    agent_config = {}
    agent_config['number_of_workers'] = {}
    agent_config['number_of_workers']['ExecWorker'] = 1

    sessions = {}

    # Enable/Disable profiling
    profiling=True

    backend = 'ORTE'

    cu_sleep = 60

    generations = 5

    # The number of cores to acquire on the resource
    nodes_var = [1, 2, 4, 8, 16, 32, 64, 128, 256]
    random.shuffle(nodes_var)

    # Single core and multicore
    cu_cores_var = [1, 32]
    random.shuffle(cu_cores_var)

    # Maximum walltime for experiment
    pilot_runtime = 30 # should we guesstimate this?

    for iter in range(repeat):

        for nodes in nodes_var:

            pilot_cores = int(resource_config[backend]['PPN']) * nodes

            for cu_cores in cu_cores_var:

                # Don't need full node experiments for low number of nodes,
                # as we have no equivalent in single core experiments
                if nodes < cu_cores:
                    continue

                # keep core consumption equal (4 generations)
                cu_count = (generations * pilot_cores) / cu_cores

                sid = run_experiment(
                    backend=backend,
                    pilot_cores=pilot_cores,
                    pilot_runtime=pilot_runtime,
                    cu_runtime=cu_sleep,
                    cu_cores=cu_cores,
                    cu_count=cu_count,
                    profiling=profiling,
                    agent_config=agent_config
                )

                sessions[sid] = {
                    'backend': backend,
                    'pilot_cores': pilot_cores,
                    'pilot_runtime': pilot_runtime,
                    'cu_runtime': cu_sleep,
                    'cu_cores': cu_cores,
                    'cu_count': cu_count,
                    'profiling': profiling,
                    'iteration': iter,
                    'number_of_workers': agent_config['number_of_workers']['ExecWorker']
                }
                f.write('%s - %s\n' % (sid, str(sessions[sid])))
                f.flush()

    f.close()
    return sessions
#
#-------------------------------------------------------------------------------

#------------------------------------------------------------------------------
#
# Variable CU duration (60)
# Fixed backend (ORTE)
# Variable CU count (5 generations)
# Variable CU cores (1, 32)
# CU = /bin/sleep
# Variable Pilot cores (256, 512, 1024, 2048, 4096, 8192)
#
# Goals: A) Investigate the scale of things. 
#        B) Investigate the effect of 1 per node vs 32 per node
#
def exp5(repeat):

    f = open('exp5.txt', 'a')
    f.write('%s\n' % time.ctime())

    agent_config = {}
    agent_config['number_of_workers'] = {}
    agent_config['number_of_workers']['ExecWorker'] = 8

    sessions = {}

    # Enable/Disable profiling
    profiling=True

    backend = 'TITAN'

    generations = 1

    # The number of cores to acquire on the resource
    #nodes_var = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]
    #nodes_var = [4, 8, 16, 32, 64, 128, 256, 512, 1024]
    nodes_var = [256]
    #random.shuffle(nodes_var)

    # Single core and multicore
    #cu_cores_var = [1, resource_config[backend]['PPN']]
    #random.shuffle(cu_cores_var)
    cu_cores_var = [1]
	
    # Maximum walltime for experiment
    pilot_runtime = 180 # should we guesstimate this?

    cu_sleep = 3600

    for iter in range(repeat):

        for nodes in nodes_var:
            
            pilot_cores = int(resource_config[backend]['PPN']) * nodes

            for cu_cores in cu_cores_var:
                
                # Don't need full node experiments for low number of nodes,
                # as we have no equivalent in single core experiments
                if nodes < cu_cores:
                    continue

                # keep core consumption equal
                cu_count = (generations * pilot_cores) / cu_cores

                #cu_sleep = max(60, cu_count / 5)

                sid = run_experiment(
                    backend=backend,
                    pilot_cores=pilot_cores,
                    pilot_runtime=pilot_runtime,
                    cu_runtime=cu_sleep,
                    cu_cores=cu_cores,
                    cu_count=cu_count,
                    profiling=profiling,
                    agent_config=agent_config
                )

                sessions[sid] = {
                    'backend': backend,
                    'pilot_cores': pilot_cores,
                    'pilot_runtime': pilot_runtime,
                    'cu_runtime': cu_sleep,
                    'cu_cores': cu_cores,
                    'cu_count': cu_count,
                    'profiling': profiling,
                    'iteration': iter,
                    'number_of_workers': agent_config['number_of_workers']['ExecWorker']
                }
                f.write('%s - %s\n' % (sid, str(sessions[sid])))
                f.flush()

    f.close()
    return sessions
#
#-------------------------------------------------------------------------------


#------------------------------------------------------------------------------
#
# Variable CU duration (0, 600, 3600)
# Fixed backend (ORTE)
# Variable CU count (1 generations)
# Variable CU cores = pilot cores
# CU = /bin/sleep
# Variable Pilot cores (32(2), 128(4), 512(32), 1024(64), 2048(128), 4096, 8192)
#
# Goals: A) Investigate the scale of things. 
#        B) Investigate the effect of 1 per node vs 32 per node
#
def exp6(repeat):

    f = open('exp6.txt', 'a')
    f.write('%s\n' % time.ctime())

    agent_config = {}
    agent_config['number_of_workers'] = {}
    agent_config['number_of_workers']['ExecWorker'] = 8

    sessions = {}

    # Enable/Disable profiling
    profiling=True

    backend = 'TITAN'

    generations = 1

    # The number of cores to acquire on the resource
    #nodes_var = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]
    #nodes_var = [4, 8, 16, 32, 64, 128, 256, 512, 1024]
    cores_var = [32, 128, 512, 1024, 2048, 4096]
    #random.shuffle(nodes_var)

    # Single core and multicore
    #cu_cores_var = [1, resource_config[backend]['PPN']]
    #random.shuffle(cu_cores_var)
    cu_cores_var = [1]
	
    # Maximum walltime for experiment
    pilot_runtime = 60 # should we guesstimate this?

    cu_sleep = 3600

    for iter in range(repeat):

        for nodes in nodes_var:
            
            pilot_cores = int(resource_config[backend]['PPN']) * nodes

            for cu_cores in cu_cores_var:
                
                # Don't need full node experiments for low number of nodes,
                # as we have no equivalent in single core experiments
                if nodes < cu_cores:
                    continue

                # keep core consumption equal
                cu_count = (generations * pilot_cores) / cu_cores

                #cu_sleep = max(60, cu_count / 5)

                sid = run_experiment(
                    backend=backend,
                    pilot_cores=pilot_cores,
                    pilot_runtime=pilot_runtime,
                    cu_runtime=cu_sleep,
                    cu_cores=cu_cores,
                    cu_count=cu_count,
                    profiling=profiling,
                    agent_config=agent_config
                )

                sessions[sid] = {
                    'backend': backend,
                    'pilot_cores': pilot_cores,
                    'pilot_runtime': pilot_runtime,
                    'cu_runtime': cu_sleep,
                    'cu_cores': cu_cores,
                    'cu_count': cu_count,
                    'profiling': profiling,
                    'iteration': iter,
                    'number_of_workers': agent_config['number_of_workers']['ExecWorker']
                }
                f.write('%s - %s\n' % (sid, str(sessions[sid])))
                f.flush()

    f.close()
    return sessions
#
#-------------------------------------------------------------------------------

#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    #sessions = exp1(3)
    #sessions = exp2(3)
    #sessions = exp3(3)
    #sessions = exp4(3)
    sessions = exp5(1)
    #sessions = exp6(1)
    print sessions
