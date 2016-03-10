
import datetime
import os
import sys
import tempfile
import time
import subprocess
import shutil
import getpass
import argparse

BOOTSTRAPPER = "bootstrap_hadoop2.py"

INIT_FILE = "scripts/hadoop-init.sh"
RUN_FILE = "scripts/hadoop-run.sh"
CLEAN_FILE = "scripts/hadoop-cleanup.sh"
CLEAN_FILE = "scripts/clearnodes.sh"

VERSION = "2.6.2"


class JobDisappeared(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class ClusterStarter:

    def __init__(self, options):
        self.id = None
        self.num_nodes = options["numnodes"]
        self.time = options["time"]  # minutes
        self.jobname = options["jobname"]
        
        d = datetime.datetime.now()
        suffix = d.strftime("%y.%m.%d-%H.%M")
        self.working_dir = os.path.join(options["workdir"], self.jobname+"-"+suffix)
        
        self.script = options['script']
        
        # script presence means auto-mode
        self.auto = False
        if self.script:
            self.auto = True
            self.script = os.path.abspath(self.script)
            print "corrected script path:", self.script

        self.qosdebug = options["dqos"]
        self.haswell = options["haswell"]
        
        return

    def create_pbs_script(self):
        """
        """
        
        jobdir = self.working_dir
        numnodes = self.num_nodes
        time = self.time
        
        # time
        walltime = "%s:%s:00" % (int(time)/60, int(time) % 60)
        
        # executable
        # executable = sys.executable
        # arguments = ["-m", "bootstrap_hadoop2"]
        executable = "python"
        arguments = [BOOTSTRAPPER]
    
        # log files
        outfile = os.path.join(jobdir, "hadoop_job.stdout")
        errfile = os.path.join(jobdir, "hadoop_job.stderr")
    
        # -----------
        # PBS TEMPLATE
        # -----------
        
        template = ""
        template += "#!/bin/bash -l\n"  # to be able to use modules of VSC
        # template += "#PBS -V" # take over our environment vars
        template += "#PBS -N %s\n" % (self.jobname)
        template += "#PBS -o %s\n" % (outfile)
        template += "#PBS -e %s\n" % (errfile)
        template += "#PBS -l walltime=%s\n" % (walltime)
        template += "#PBS -l pmem=2gb\n"
        template += "#PBS -A lp_h_gumbo\n" # TODO set account as parameter
        # template += "#PBS -q %s\n"%s(queue)
        # nodes
        if self.qosdebug:
            template += "#PBS -l nodes=%s:ppn=20\n"%(numnodes)
        elif self.haswell:
            template += "#PBS -l nodes=%s:ppn=24:haswell\n"%(numnodes)
            template += "#PBS -l feature=mem64\n"
        else:
            template += "#PBS -l nodes=%s:ppn=20:ivybridge\n"%(numnodes)
            template += "#PBS -l feature=mem64\n"
    
        template += "#PBS -M jonnydaenen@gmail.com\n"
        if self.qosdebug:
            template += "#PBS -l qos=debugging\n"
        template += "\n"
        # modules
        template += "# modules\n" 
        template += "module load Java/1.7.0_51\n"
        template += "module load Python/2.7.6-intel-2014a\n"
        template += "\n"
        # load scripts
        template += "# environment\n" 
        # template += "export PBS_O_WORKDIR=%s\n"%(jobdir)
        template += "mkdir -p %s\n" % (jobdir)
        template += "cd %s\n" % (jobdir)
        template += "mkdir work\n"
        template += "touch work/allocated\n"
        template += "echo pwd=`pwd` >> log.txt\n"
        template += "echo HOME=$HOME >> log.txt\n"
        template += "echo JAVA_HOME=$JAVA_HOME >> log.txt\n"
        template += "\n"
        # execute scripts
        template += "# execute scripts\n"
        args = reduce(lambda x, y: str(x)+" " + str(y), arguments)
        template += "echo '%s %s' >> log.txt\n" % (executable, args)
        template += "%s %s > bootstrap.out 2> bootstrap.err\n" % (executable, args)
    
        return template

    def copy_bootstrapper(self):
        """
        Copies the bootstrap script to the working dir.
        """
        
        # copy bootstrapper
        src = os.path.join(os.path.dirname(os.path.realpath(__file__)), BOOTSTRAPPER)
        dst = os.path.join(self.working_dir, BOOTSTRAPPER)
        shutil.copyfile(src, dst)

        # TODO copy dependencies

        # copy hadoop package
        src = os.path.join(os.path.dirname(os.path.realpath(__file__)), "hadoop.tar.gz")
        dst = os.path.join(self.working_dir, "work/hadoop.tar.gz")
        if os.path.exists(src):
            shutil.copyfile(src, dst)

    def start_cluster(self):
        """
        starts a hadoop cluster using a pbs script
        and the bootstrap script.
        """
        
        # create folders
        print "creating work and script dir..."
        
        workpath = os.path.join(self.working_dir, "work")
        scriptpath = os.path.join(self.working_dir, "scripts")
        for p in [workpath, scriptpath]:
            if not os.path.exists(p):
                os.makedirs(p)
        
        # create pbs script
        print "creating PBS script ... "
        pbs_script = self.create_pbs_script()
        
        # put script in temp file
        # temp = tempfile.NamedTemporaryFile(suffix='.pbs', delete=False)
        # temp.write(pbs_script)
        # temp.close()
        # print "location:", temp.name
        
        pbs_path = os.path.join(self.working_dir, "scripts/job.pbs")
        f = open(pbs_path, "w")
        f.write(pbs_script)
        f.close()
        print "Job script is stored in:", pbs_path
    
        
        
        # copy bootstrap file
        self.copy_bootstrapper()

        self.copy_control_files()

        # submit job using qsub
        print "submitting job..."
        output = subprocess.check_output(['qsub', pbs_path])
        self.id = output[:output.find(".")]
        
        print "job submitted with id: ", self.id
        
        try:
            # print settings only when the cluster was setup correctly
            if self.wait_for_job():
                self.export_cluster_settings()
                
        except JobDisappeared as error:
            print "reason:", error
            print "job disappeared!"
            self.cancel_job()
            
        
    def wait_for_job(self):
        """
        Waits for allocation, then for start of hadoop cluster.
        """
        
        # the allocated path is created when hadoop is ready
        # this is done by the PBS script
        allocatedpath = os.path.join(self.working_dir, "work/allocated")

        print "Waiting for node allocation"
        allocated = self.wait_for_path(allocatedpath, True)
        print "Nodes allocated."
        
        if not allocated:
            return False

        # the started path is created when hadoop is ready
        # this is done by the bootstrap script
        startedpath = os.path.join(self.working_dir, "work/started")

        
        print "Initializing Hadoop cluster"
        started = self.wait_for_path(startedpath)
        print "Hadoop clusted initialized (wait a few minutes to make sure it's entirely up and running)."
        
        return started
        

    def wait_for_path(self, path, print_est_start_time=False):
        """
        Checks if a specific file/directory to appear.
        Returns true if the file/folder is present,
        false when the job is not running/in the queue anymore.
        """

        found = False
        jobgone = 10 # how many times to wait for a missing job
        # wait for the directory to appear
        # or for the job to disappear from the queue
        i = 0
        sleep = 5
        while True:
            
            # print est. start time, or dots
            if print_est_start_time:
                    print "Estimated start time: ", self.get_estimated_start_time(self.id)
            else:
                if i % 10 == 0:
                    sys.stdout.write("|")
                else:
                    sys.stdout.write(".")
                    
                if i % 60 == 0:
                    sys.stdout.write("(%s min)\n"%(i/60))
                    
                sys.stdout.flush()
            
            try:
                devnull = open(os.devnull, 'w')
                executeOutput = subprocess.Popen("ls %s" % (path), shell=True, stdout=subprocess.PIPE, stderr=devnull)
                while executeOutput.poll() is None:
                    pass
                output = executeOutput.stdout.readlines()
                
                if len(output) == 1:
                    found = True
                    break

            except subprocess.CalledProcessError:
                print "Warning: error while checking file, still using fall-back mechanisms"
            
            if os.path.exists(path):
                found = True
                break
            if not self.is_queued(self.id):
                jobgone -= 1
                print "Job is not present in queue system, %s more checks remaining"%(str(jobgone))
                if jobgone <= 0:
                    raise JobDisappeared("ERROR: Job not found in queue system!")
                    # print "ERROR: Job not found in queue system!"
                    # break

            i += sleep
            time.sleep(sleep)
            
        # add newline to end of progress dots
        if not print_est_start_time:
            print ""
        
        return found
        
        
    def copy_control_files(self):
        # copy hadoop control scripts
        files = [INIT_FILE, RUN_FILE, CLEAN_FILE]
        for file in files:
            src = os.path.join(os.path.dirname(os.path.realpath(__file__)), file)
            dst = os.path.join(self.working_dir, file)  # script dir is in name
            shutil.copyfile(src, dst)

    def export_cluster_settings(self):
        """
        create shell script in working dir that sets up the environment
        to operate on cluster. 
        """

        print "Generating cluster control scripts..."
    
        # create script dir
        script_dir = os.path.join(self.working_dir, "scripts")
        
        # create enviroment setup script
        script = self.get_hadoop_env_script(self.id, self.working_dir)
        
        env_file = open(os.path.join(script_dir, "hadoop-env.sh"), "w")
        env_file.write(script)
        env_file.close()
        
        # # copy hadoop control scripts
 #        files = [INIT_FILE, RUN_FILE, CLEAN_FILE]
 #        for file in files:
 #            src = os.path.join(os.path.dirname(os.path.realpath(__file__)), file)
 #            dst = os.path.join(self.working_dir, file)  # script dir is in name
 #            shutil.copyfile(src, dst)
        
        print "Scripts generated."
        
        print "Allocated nodes:"

        startedpath = os.path.join(self.working_dir, "work/nodes")
        nodefile = open(startedpath, "r")
        print nodefile.read()
        
        # TODO add auto/manual mode
        print
        print "Manual commands"
        print "---------------"
        print "Use the following commands to navigate to the working dir and set up a shell:"
        # print "ssh %s"%(self.get_master_node())
        # print "sh %s"%os.path.join(self.working_dir,INIT_FILE) # script dir is in name
        print "ssh -t %s 'source %s; bash -l'" % (self.get_master_node(),
                                                  os.path.join(self.working_dir, INIT_FILE))  # script dir is in name
        print
        
        print "To clean up:"
        print "bash -l %s" % os.path.join(self.working_dir, CLEAN_FILE)  # script dir is in name
        print
        
        # manual mode command :
        print "Auto commands"
        print "-------------"
        print "ssh %s 'bash -l %s'" % (self.get_master_node(),
                                       os.path.join(self.working_dir, RUN_FILE))  # script dir is in name
        print
        
        if self.auto:
            errfile = open(os.path.join(self.working_dir, "auto.err"), "w")
            outfile = open(os.path.join(self.working_dir, "auto.out"), "w")
            
            p = subprocess.Popen(["ssh", self.get_master_node(), 'bash -l %s' %
                                  os.path.join(self.working_dir, RUN_FILE)], stdout=outfile, stderr=errfile)
            
            # wait for completion
            p.wait()
            
            # TODO intermediate flushing:
            # while p.poll() is None:
            #    sleep(1)
                
            # flush and close when its done
            errfile.flush()
            errfile.close()
            
            outfile.flush()
            outfile.close()
        
    def get_master_node(self):
        masternode_path = os.path.join(self.working_dir, "work/masternode")
        nodefile = open(masternode_path, "r")
        master = str(nodefile.read()).strip()
        return master

    def get_hadoop_env_script(self, jobid, working_directory):
        
        hosts = self.get_exec_host(jobid)
        print "hosts:", hosts
        
        hadoop_home = os.path.join(working_directory, "work/hadoop-"+VERSION)
        
        init_script = ""
        init_script += "# HADOOP installation directory: %s\n " % hadoop_home
        # print "Allocated Resources for Hadoop cluster: " + hosts
        # print "YARN Web Interface: http://%s:8088"% hosts[:hosts.find("/")]
        # print "HDFS Web Interface: http://%s:50070"% hosts[:hosts.find("/")]
        init_script += "# (please allow some time until the Hadoop cluster is completely initialized)\n\n"
        
        init_script += "# To use Hadoop set HADOOP_CONF_DIR: \n"
        init_script += "export HADOOP_CONF_DIR=%s\n" % (os.path.join(working_directory, "work",
                                                        self.get_most_current_job(working_directory), "etc/hadoop"))
        init_script += "export HADOOP_HOME=%s\n" % (hadoop_home)
        init_script += "export PATH=%s/bin:$PATH\n" % (hadoop_home)

        init_script += "# other env vars: \n"
        init_script += self.get_env_vars()
  
        return init_script
        
    def get_env_vars(self):
        """
        Returns a bash script that sets the env vars.
        """
        
        table = [
            ('HADOOP_WORKDIR', self.working_dir),
            ('HADOOP_JOBID', self.id),
            ('HADOOP_USERNAME', getpass.getuser()),
            ('HADOOP_RUNSCRIPT', self.script),  # TODO add script options
            ('HADOOP_MASTER_NODE', self.get_master_node())
        ]
        
        envscript = ""
        for v in table:
            envscript += "export %s=%s\n" % v
        
        return envscript

    def get_most_current_job(self, working_directory):
        dir = os.path.join(working_directory, "work")
        files = os.listdir(dir)
        max = None
        for i in files:
            if i.startswith("hadoop-conf"):
                t = os.path.getctime(os.path.join(dir, i))
                if (max is None) or t > max[0]:
                    max = (t, i)
        return max[1]       
    
    def get_estimated_start_time(self, pbs_id):
        starttime = "unknown"
        try:
            output = subprocess.check_output(["showstart", pbs_id])
            for line in output.split("\n"):
                # print line
                if line.find("start in") > 0:
                    starttime = line[line.find(":")-2:].strip()
                    break
        except:
            pass
            
        return starttime

    def get_exec_host(self, pbs_id):
        hosts = "localhost/"
        try:
            nodes = subprocess.check_output(["qstat", "-f", pbs_id])
            for i in nodes.split("\n"):
                if i.find("exec_host") > 0:
                    hosts = i[i.find("=")+1:].strip()
        except:
            pass
            
        return hosts

    def cancel_job(self, pbs_id):
        
        try:
            subprocess.check_output(["qdel", self.id])
        except:
            pass
        
    def is_queued(self, pbs_id):
        
        try:
            output = subprocess.check_output(["qstat", "-a", pbs_id])
            lines = output.split("\n")
            if len(lines) > 1:  # 1 line can indicate unknown job error
                return True
            else:
                return False
        except:
            pass
            
        # problem reading queue
        return False

    def get_allocated_nodes(self, pbs_id):
        hosts = "localhost/"
        try:
            nodes = subprocess.check_output(["qstat", "-f", pbs_id])
            for i in nodes.split("\n"):
                if i.find("exec_host") > 0:
                    hosts = i[i.find("=")+1:].strip()
        except:
            pass
            
        return hosts


def main():
    parser = argparse.ArgumentParser(description='Setup hadoop cluster on PBS cluster.')
    parser.add_argument("-w", "--workdir", action='store', required=True,
                        help="the workdir for the cluster, on shared disk")
    parser.add_argument("-n", "--numnodes", action='store', type=int, default=3,
                        help="the number of nodes")
    parser.add_argument("-t", "--time", action='store', default="30",
                        help="the maximum time in minutes to keep the cluster running")
    parser.add_argument("-j", "--jobname", action='store', default="hadoop-beta",
                        help="the name of the job")
    parser.add_argument("-s", "--script", action='store',
                        help="the script to execute (auto-mode)", default=None)
    parser.add_argument("--dqos", action='store_true', default=False,
                        help="request debug job qos")
    parser.add_argument("--haswell", action='store_true', default=False,
                        help="request haswell nodes")
    parser.add_argument("--bootstrapper", action='store', required=False, default="bootstrap_hadoop2.py",
                        help="the bootstrap file to be used")
    args = parser.parse_args()
    # print args
    # print args.accumulate(args.integers)
    
    print reduce(lambda x, y: x + "\n\t" + y + ": " + str(vars(args)[y]), vars(args), "Settings:")
    
    # get parameters
    params = vars(args)
    
    global BOOTSTRAPPER
    BOOTSTRAPPER = params['bootstrapper']
    
    # start cluster
    cs = ClusterStarter(params)
    cs.start_cluster()

        
if __name__ == '__main__':
    main()
