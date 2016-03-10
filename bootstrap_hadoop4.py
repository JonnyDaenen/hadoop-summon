#!/usr/bin/env python
""" Hadoop Bootstrap Script (based on SAGA-hadoop) """
import os, sys
import pdb
import urllib
import subprocess
import logging
import uuid
import shutil
import time
import signal
import socket
from tempfile import mkstemp
from optparse import OptionParser
import socket

logging.basicConfig(level=logging.DEBUG)

# For automatic Download and Installation
VERSION = "2.6.2"
HADOOP_DOWNLOAD_URL = "http://apache.osuosl.org/hadoop/common/hadoop-" + VERSION + "/hadoop-" + VERSION + ".tar.gz"
WORKING_DIRECTORY = os.path.join(os.getcwd(), "work")

# For using an existing installation
HADOOP_HOME = os.path.join(os.getcwd(), "work/hadoop-" + VERSION)
HADOOP_CONF_DIR = os.path.join(HADOOP_HOME, "etc/hadoop")

HDFS_NAMEDIR = "/node_scratch/hdfs/name/"
HDFS_DATADIR = "/node_scratch/hdfs/data/"

STOP = False


def handler(signum, frame):
    logging.debug("Signal caught. Stop Hadoop")  # TODO does hadoop stop automatically
    global STOP
    STOP = True


# src: http://stackoverflow.com/questions/39086/search-and-replace-a-line-in-a-file-in-python
def replace(file_path, pattern, subst):
    # Create temp file
    fh, abs_path = mkstemp()
    new_file = open(abs_path, 'w')
    old_file = open(file_path)
    for line in old_file:
        new_file.write(line.replace(pattern, subst))
    # close temp file
    new_file.close()
    os.close(fh)
    old_file.close()
    # Remove original file
    os.remove(file_path)
    # Move new file
    shutil.move(abs_path, file_path)


class Hadoop2Bootstrap(object):
    def __init__(self, working_directory):
        self.working_directory = working_directory
        self.jobid = "hadoop-conf-" + str(uuid.uuid1())
        self.job_working_directory = os.path.join(working_directory, self.jobid)
        self.job_conf_dir = os.path.join(self.job_working_directory, "etc/hadoop/")
        self.job_name_dir = os.path.join(self.job_working_directory, "name")
        self.job_log_dir = os.path.join(self.job_working_directory, "logs")
        try:
            # os.makedirs(self.job_conf_dir)
            os.makedirs(self.job_name_dir)
            os.makedirs(self.job_log_dir)

            # create remote dirs
            self.create_dirs()
        except:
            print("error creating dirs...")
            pass

    def get_core_site_xml(self, hostname):
        return """<?xml version="1.0"?>
    <configuration>
        <property>
            <name>fs.default.name</name>
            <value>hdfs://%s:9000</value>
            <description>Deprecated. Use (fs.defaultFS) property instead</description>
        </property>
        
        <property>
            <name>fs.defaultFS</name>
            <value>hdfs://%s:9000</value>
        </property>
        
        <property>
            <name>hadoop.tmp.dir</name>
            <value>/node_scratch/hadoop-${user.name}</value>
            <description></description>
        </property>
        
        <property>
            <name>io.file.buffer.size</name>
            <value>131072</value>
            <description></description>
        </property>
        
    </configuration>""" % (hostname, hostname)

    def get_core_site_xml_old(self, hostname):
        return """<?xml version="1.0"?>
    <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
    <configuration>
         <property>
             <name>fs.default.name</name>
             <value>hdfs://%s:9000</value>
         </property>
    </configuration>""" % (hostname)

    def get_hdfs_site_xml(self, hostname, name_dir):
        return """<?xml version="1.0"?>
    <configuration>
        <property>
            <name>dfs.namenode.name.dir</name>
            <value>file://%s</value>
            <final>true</final>
        </property>
        <property>
            <name>dfs.datanode.data.dir</name>
            <value>file://%s</value>
            <final>true</final>
        </property>
        <property>
            <name>dfs.namenode.checkpoint.dir</name>
            <value>file:///node_scratch/tmp/namesecondary</value>
            <final>true</final>
        </property>
        <property>
          <name>dfs.replication</name>
          <value>3</value>
        </property>
        <property>
            <name>dfs.webhdfs.enabled</name>
            <value>true</value>
            <description>???</description>
        </property>
        <!--
        <property>
            <name>dfs.datanode.handler.count</name>
            <value>20</value>
        </property>
        -->
    </configuration>""" % (HDFS_NAMEDIR, HDFS_DATADIR)

    def get_hdfs_site_xml_old(self, hostname, name_dir):
        return """<?xml version="1.0"?>
    <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
    <configuration>
         <property>
             <name>dfs.replication</name>
             <value>1</value>
         </property>
         <property>
             <name>dfs.name.dir</name>
             <value>%s</value>
         </property>
         <!--property>
             <name>dfs.datanode.dns.interface</name>
             <value>eth1</value>
         </property-->
          <property>
               <name>dfs.datanode.data.dir</name>
               <value>/node_scratch/datanode</value>
         </property>
         <property>
              <name>dfs.datanode.data.dir.perm</name>
              <value>700</value>
              <description>Permissions for the directories on on the local filesystem where
              the DFS data node store its blocks. The permissions can either be octal or
                symbolic.</description>
        </property>     
         <property>
             <name>dfs.webhdfs.enabled</name>
             <value>true</value>
         </property>         
    </configuration>""" % (name_dir)

    def get_mapred_site_xml(self, hostname):
        return """<?xml version="1.0"?>
        <configuration>
        
            <property>
                <name>mapreduce.framework.name</name>
                <value>yarn</value>
                <description>use the yarn framework instead of local</description>
            </property>
        
        
            <property>
                <name>mapred.child.java.opts</name>
                <value>-Xmx1024m</value>
                <description>Java heap size, should be smaller than the 2 below</description>
                <!-- Not marked as final so jobs can include JVM debugging options -->
            </property>
        
            <property>
                <name>mapreduce.map.memory.mb</name>
                <value>1280</value>
                <description>
                    The physical amount of memory to be allocated for a map job.
                    Note this also takes into account java vm overhead 
                </description>
            </property>
        
            <property>
                <name>mapreduce.reduce.memory.mb</name>
                <value>1280</value>
                <description>The physical amount of memory to be allocated for a reduce job</description>
            </property>
            
            <property>
                <name>mapreduce.task.io.sort.mb</name>
                <value>50</value>
                <description>size of the map-side sort buffer</description>
            </property>
            
            <property>
                <name>mapreduce.reduce.merge.inmem.threshold</name>
                <value>0</value>
                <description>only write reduce data to disk when memory is full</description>
            </property>
            
            <property>
                <name>mapreduce.reduce.input.buffer.percent</name>
                <value>0.5</value>
                <description>50pct of heap can be used to keep data in memory</description>
            </property>
            
            <property>
                <name>mapreduce.job.reduce.slowstart.completedmaps</name>
                <value>1</value>
                <description>percentage of mappers that must be complete before starting the first reducers</description>
            </property>

            <!--
            <property>
                <name>mapreduce.map.sort.spill.percent</name>
                <value>0.9</value>
            </property>
            -->
            
            
            <property>
                <name>mapreduce.task.io.sort.factor</name>
                <value>2</value>
            </property>
            

            <!--
            <property>
                <name>mapreduce.map.output.compress</name>
                <value>true</value>
                <description>this slows down the mapper by factor 1.5</description>
            </property>
            -->
            <!--
            <property>
                <name>mapreduce.reduce.shuffle.parallelcopies</name>
                <value>10</value>
                <description>default = 5</description>
            </property>
            -->
        </configuration>
        """

    def get_mapred_site_xml_old(self, hostname):
        return """<?xml version="1.0"?>
        <configuration>
         <property>
             <name>mapred.job.tracker</name>
             <value>%s:9001</value>
             <description>the address of the jobtracker</description>
         </property>
         
         
         <!-- VSC -->
         
         <property>
           <name>mapreduce.map.memory.mb</name>
           <value>2816</value>
           <description></description>
         </property>
         
         <property>
           <name>mapreduce.reduce.memory.mb</name>
           <value>5632</value>
           <description></description>
         </property>
         
         <property>
           <name>mapreduce.map.java.opts</name>
           <value>-Xmx2048M</value>
           <description></description>
         </property>
         
         <property>
           <name>mapreduce.reduce.java.opts</name>
           <value>-Xmx4096M</value>
           <description></description>
         </property>
         
         
         <property>
           <name>yarn.app.mapreduce.am.resource.mb</name>
           <value>5632</value>
           <description>??</description>
         </property>
  
         <property>
           <name>yarn.app.mapreduce.am.command-opts</name>
           <value>-Xmx4505m</value>
           <description>??</description>
         </property>
         
        
    </configuration>""" % (hostname)

    def get_yarn_site_xml(self, hostname):
        return """<?xml version="1.0"?>
            <configuration>
                <property>
                    <name>yarn.resourcemanager.hostname</name>
                    <value>%s</value>
                    <description>Location of the resource manager</description>
                </property>
                <property>
                    <name>yarn.nodemanager.local-dirs</name>
                    <value>/node_scratch/tmp/nm-local-dir</value>
                    <final>true</final>
                    <description>which local disks to store intermediate data on</description>
                </property>
                <property>
                    <name>yarn.nodemanager.aux-services</name>
                    <value>mapreduce_shuffle</value>
                    <description>Enable shufflers to send map output to reduce tasks</description>
                </property>
                
                <property>
                    <name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name>
                    <value>org.apache.hadoop.mapred.ShuffleHandler</value>
                    <description>should fix a bug with stuck reducers, see https://www.mail-archive.com/yarn-dev@hadoop.apache.org/msg06940.html</description>
                </property>
                
                <property>
                    <name>yarn.nodemanager.resource.memory-mb</name>
                    <value>49152</value> <!-- old: 56320 -->
                    <description>
                        Memory per node, total memory minus space for system, 
                        nodemanager and datanode requirements
                    </description>
                </property>
                <!--
                <property>
                    <name>yarn.scheduler.minimum-allocation-mb</name>
                    <value>4096</value>
                <property>
                -->
                <property>
                    <name>yarn.scheduler.maximum-allocation-mb</name>
                    <value>49152</value>
                    <description>
                        ???
                    </description>
                </property>

                <property>
                  <name>yarn.nodemanager.resource.cpu-vcores</name>
                  <value>10</value>
                  <description>Number of cores available per node</description>
                </property>
                
                <property>
                  <name>yarn.log-aggregation-enable</name>
                  <value>false</value>
                  <description>???</description>
                </property>
                
                
            </configuration>
        """ % (hostname)

    def get_yarn_site_xml_old(self, hostname):
        return """<?xml version="1.0"?>
<configuration>
  <property>
    <name>yarn.resourcemanager.scheduler.class</name>
    <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler</value>
    <description>In case you do not want to use the default scheduler</description>
  </property>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>%s</value>
    <description>In case you do not want to use the default scheduler</description>
  </property>
  <property>
    <name>yarn.nodemanager.local-dirs</name>
    <value></value>
    <description>the local directories used by the nodemanager</description>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
    <description>shuffle service that needs to be set for Map Reduce to run </description>
  </property>
  <property>
    <name>yarn.nodemanager.delete.debug-delay-sec</name>
    <value>3600</value>
    <description>delay deletion of user cache </description>
  </property>
  
  <!-- new config for VSC cluster -->
  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>56320</value>
    <description>Actual memory available on a node, in mb</description>
  </property>
  
  <property>
    <name>yarn.scheduler.minimum-allocation-mb</name>
    <value>2816</value>
    <description>Minimal memory that is allocated for a container (in mb)</description>
  </property>
  
  <property>
    <name>yarn.scheduler.maximum-allocation-mb</name>
    <value>56320</value>
    <description>Maximal memory that can be requested for a container (in mb)</description>
  </property>
  
  
  <property>
    <name>yarn.nodemanager.resource.cpu-vcores</name>
    <value>10</value>
    <description>Number of cores per node</description>
  </property>
  
  <property>
    <name>yarn.scheduler.minimum-allocation-vcores</name>
    <value>1</value>
    <description></description>
  </property>
  
  <property>
    <name>yarn.scheduler.maximum-allocation-vcores</name>
    <value>4</value>
    <description></description>
  </property>
  
  
</configuration>""" % (hostname)

    def create_dirs(self):
        nodes = self.get_pbs_allocated_nodes()

        # remotely create directories on each node
        # for the namenode and datanode
        for node in nodes:
            # os.system("ssh %s -T 'mkdir -p %s'"%(node,HDFS_NAMEDIR))
            # os.system("ssh %s -T 'mkdir -p %s'"%(node,HDFS_DATADIR))
            pass

    def write_nodefile(self):

        list = self.get_pbs_allocated_nodes()

        location = os.path.join(self.working_directory, "masternode")

        f = open(location, "w")
        f.write("%s" % (socket.gethostname()))
        f.close()

        location = os.path.join(self.working_directory, "nodes")

        f = open(location, "w")
        for i in list:
            f.write(str(i))
        f.close()

    def get_pbs_allocated_nodes(self):
        print("Init PBS")
        pbs_node_file = os.environ.get("PBS_NODEFILE")
        if pbs_node_file == None:
            return ["localhost"]
        f = open(pbs_node_file)
        nodes = f.readlines()
        for i in nodes:
            i.strip()
        f.close()
        return list(set(nodes))

    def get_sge_allocated_nodes(self):
        print("Init SGE")
        sge_node_file = os.environ.get("PE_HOSTFILE")
        if sge_node_file == None:
            return ["localhost"]
        f = open(sge_node_file)
        sgenodes = f.readlines()
        f.close()
        nodes = []
        for i in sgenodes:
            columns = i.split()
            try:
                for j in range(0, int(columns[1])):
                    print("add host: " + columns[0].strip())
                    nodes.append(columns[0] + "\n")
            except:
                pass
        nodes.reverse()
        return list(set(nodes))

    def configure_hadoop(self):

        # copy default config from hadoop installation
        logging.debug("Copy config from " + HADOOP_CONF_DIR + " to: " + self.job_conf_dir)
        shutil.copytree(HADOOP_CONF_DIR, self.job_conf_dir)

        # write allocated nodes into master/slave files
        if (os.environ.get("PBS_NODEFILE") != None and os.environ.get("PBS_NODEFILE") != ""):
            nodes = self.get_pbs_allocated_nodes()
        else:
            nodes = self.get_sge_allocated_nodes()

        if nodes != None:
            # master is the current node
            master = socket.gethostname()
            # master = nodes[0].strip()
            logging.debug("master: " + master)
            # master_file = open(os.path.join(self.job_conf_dir, "masters"), "w")
            # master_file.write(master)
            # master_file.close()

            # slaves is everything including master
            slave_file = open(os.path.join(self.job_conf_dir, "slaves"), "w")
            slave_file.writelines(nodes)
            slave_file.close()
            logging.debug("Hadoop cluster nodes: " + str(nodes))

        # write core config
        core_site_file = open(os.path.join(self.job_conf_dir, "core-site.xml"), "w")
        core_site_file.write(self.get_core_site_xml(master))
        core_site_file.close()

        # write hdfs config
        hdfs_site_file = open(os.path.join(self.job_conf_dir, "hdfs-site.xml"), "w")
        hdfs_site_file.write(self.get_hdfs_site_xml(master, self.job_name_dir))
        hdfs_site_file.close()

        # write mapred config
        mapred_site_file = open(os.path.join(self.job_conf_dir, "mapred-site.xml"), "w")
        mapred_site_file.write(self.get_mapred_site_xml(master))
        mapred_site_file.close()

        # write yarn config
        yarn_site_file = open(os.path.join(self.job_conf_dir, "yarn-site.xml"), "w")
        yarn_site_file.write(self.get_yarn_site_xml(master))
        yarn_site_file.close()

        # adjust java home
        env_file = os.path.join(self.job_conf_dir, "hadoop-env.sh")
        extra = "\nmkdir -p %s" % (HDFS_NAMEDIR)
        extra += "\nmkdir -p %s" % (
            HDFS_DATADIR)  # "\necho ATTENTION\necho $TMPDIR\necho $VSC_SCRATH\necho $VSC_NODE\necho $VSC_DATA\n"
        replace(env_file, "${JAVA_HOME}", os.environ["JAVA_HOME"] + extra)

        logging.info("Hadoop config in place")

    def start_hadoop(self):
        logging.info("Starting Hadoop")

        # if namenode is fresh, format it 
        if not os.environ.has_key("HADOOP_CONF_DIR") or os.path.exists(os.environ["HADOOP_CONF_DIR"]) == False:

            self.set_env()
            # format_command = os.path.join(HADOOP_HOME, "bin/hadoop") + " --config " + self.job_conf_dir + " namenode -format"
            format_command = os.path.join(HADOOP_HOME,
                                          "bin/hdfs") + " --config " + self.job_conf_dir + " namenode -format"

            logging.info("Formatting namenode")
            logging.debug("Execute: %s" % format_command)

            os.system("rm -rf /tmp/hadoop-*")
            os.system(format_command)

        else:
            logging.debug("Don't format namenode. Reconnect to existing namenode")

        # set conf dir etc.
        self.set_env()

        logging.debug("JAVA_HOME=" + os.environ['JAVA_HOME'])
        logging.debug("HADOOP_CONF_DIR=" + os.environ['HADOOP_CONF_DIR'])

        # start-up hadoop 
        # start_command = os.path.join(HADOOP_HOME, "sbin/start-all.sh")
        start_command = os.path.join(HADOOP_HOME, "sbin/start-dfs.sh")
        logging.info("Starting DFS")
        logging.debug("Execute: %s" % start_command)
        os.system(". ~/.bashrc & " + start_command)

        start_command = os.path.join(HADOOP_HOME, "sbin/start-yarn.sh")
        logging.info("Starting YARN")
        logging.debug("Execute: %s" % start_command)
        os.system(". ~/.bashrc & " + start_command)

        # history server
        start_command = os.path.join(HADOOP_HOME, "sbin/mr-jobhistory-daemon.sh") + " start historyserver"
        logging.info("Starting historyserver")
        logging.debug("Execute: %s" % start_command)
        os.system(". ~/.bashrc & " + start_command)



        # ??? why is this
        print("Hadoop started, please set HADOOP_CONF_DIR to:\nexport HADOOP_CONF_DIR=%s" % self.job_conf_dir)

    def stop_hadoop(self):
        logging.debug("Stop Hadoop")
        self.set_env()
        stop_command = os.path.join(HADOOP_HOME, "sbin/stop-all.sh")
        logging.debug("Execute: %s" % stop_command)
        os.system(stop_command)

    def start(self):
        # set configuration
        if not os.environ.has_key("HADOOP_CONF_DIR") or os.path.exists(os.environ["HADOOP_CONF_DIR"]) == False:
            logging.debug("No existing Hadoop conf dir found")
            self.configure_hadoop()
        else:
            logging.debug("Existing Hadoop Conf dir? %s" % os.environ["HADOOP_CONF_DIR"])
            self.job_conf_dir = os.environ["HADOOP_CONF_DIR"]
            self.job_log_dir = os.path.join(self.job_conf_dir, "../log")
            self.job_name_dir = os.path.join(self.job_conf_dir, "../name")

        # start hadoop
        self.start_hadoop()

    def stop(self):
        if os.environ.has_key("HADOOP_CONF_DIR") and os.path.exists(os.environ["HADOOP_CONF_DIR"]) == True:
            self.job_conf_dir = os.environ["HADOOP_CONF_DIR"]
            self.job_log_dir = os.path.join(self.job_conf_dir, "../log")
        self.stop_hadoop()

    def set_env(self):
        logging.debug("Export HADOOP_CONF_DIR to %s" % self.job_conf_dir)
        os.environ["HADOOP_CONF_DIR"] = self.job_conf_dir
        logging.debug("Export HADOOP_LOG_DIR to %s" % self.job_log_dir)
        os.environ["HADOOP_LOG_DIR"] = self.job_log_dir


#########################################################
#  main                                                 #
#########################################################
if __name__ == "__main__":

    logging.debug("Working dir process: " + WORKING_DIRECTORY)

    signal.signal(signal.SIGALRM, handler)
    signal.signal(signal.SIGABRT, handler)
    signal.signal(signal.SIGQUIT, handler)
    signal.signal(signal.SIGINT, handler)

    # set-up commandline options
    parser = OptionParser()
    parser.add_option("-s", "--start", action="store_true", dest="start",
                      help="start Hadoop", default=True)
    parser.add_option("-q", "--quit", action="store_false", dest="start",
                      help="terminate Hadoop")
    parser.add_option("-c", "--clean", action="store_true", dest="clean",
                      help="clean HDFS datanodes after termination")

    # fetch commandline options
    (options, args) = parser.parse_args()


    # info
    logging.info("Bootstrap Hadoop running on " + socket.gethostname())


    # check if dir
    if not os.path.exists(HADOOP_HOME):

        # create working dir
        try:
            os.makedirs(WORKING_DIRECTORY)
        except:
            pass

        # download hadoop
        download_destination = os.path.join(WORKING_DIRECTORY, "hadoop.tar.gz")
        if not os.path.exists(download_destination):
            logging.info("Downloading  %s to %s" % (HADOOP_DOWNLOAD_URL, download_destination))
            opener = urllib.FancyURLopener({})
            opener.retrieve(HADOOP_DOWNLOAD_URL, download_destination);
        else:
            logging.info("Found existing Hadoop binaries at: " + download_destination)

        logging.info("Extracting Hadoop")
        os.chdir(WORKING_DIRECTORY)
        os.system("tar -xzf hadoop.tar.gz")



    # execute command on this hadoop instance
    hadoop = Hadoop2Bootstrap(WORKING_DIRECTORY)
    if options.start:
        hadoop.start()
    else:
        hadoop.stop()
        if options.clean:
            directory = "/tmp/hadoop-" + os.getlogin()
            logging.debug("delete: " + directory)
            shutil.rmtree(directory)
        sys.exit(0)

    # write session info
    hadoop.write_nodefile()


    # create file to indicate that cluster is initializing
    f = open(os.path.join(WORKING_DIRECTORY, 'started'), 'w')
    f.close()

    # done, sleep until signal is caught
    print("Finished launching of Hadoop Cluster - Sleeping now")

    while not STOP:
        logging.debug("stop: " + str(STOP))
        time.sleep(10)

    # clean stop + remove started file 
    hadoop.stop()
    os.remove(os.path.join(WORKING_DIRECTORY, "started"))
