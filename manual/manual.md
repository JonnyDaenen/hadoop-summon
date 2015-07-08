# hadoop on VSC-cluster



1. connect to the cluster
1. request a cluster
1. wait for cluster allocation
1. wait for hadoop
1. connect to cluster
1. test cluster using TeraSort
1. load data onto cluster
1. running pig scripts
1. running gumbo scripts


## Connect to the Cluster
Note: this part is for VSC-cluster only.

Connecting to the cluster using the following command:

    ssh vsc30655@login.hpc.kuleuven.be
    
this will set up an ssh connection. Note that you do need a private key file
in `~/.ssh` that matches a public key which is registered to a VSC-user.

The script directory on Thinking is `~/gumbo/cluster`.

Important directories:

- user directory: /user/leuven/306/vsc30655
- data directory: /data/leuven/306/vsc30655
- scratch dir:
- node scratch dir: 

## Request a Cluster

Requesting a cluster is done using the hadoop-setup python script:

    python hadoop-setup.py

this script asks for a couple of things:

 - a directory, in which the working directory will be created
- the number of nodes (e.g. 2)
- the time the cluster needs remain online 

We will store our data in:
    
    /data/leuven/306/vsc30655/hadoop-test
    
<!-- @TODO create env var for this $HADOOP_TEST_DIR -->

Note that the working directory needs to meet certain requirements:

- needs to be accessible from the current dir
<!-- @TODO why is this needed? what are requirements for this dir? accessible? -->


When you run the script, the jobdir is created and the Hadoop bootstrap program is copied there. This will be activated once the requested nodes are aquired.

Next, a PBS script is generated, for example:

    #!/bin/bash -l
    #PBS -N hadoop-cf
    #PBS -o  /data/leuven/306/vsc30655/hadoop-test/hadoop-cf-14.10.23-11.06/hadoop_job.stdout
    #PBS -e  /data/leuven/306/vsc30655/hadoop-test/hadoop-cf-14.10.23-11.06/hadoop_job.stderr
    #PBS -l walltime=0:10:00
    #PBS -l nodes=2:ppn=20
    #PBS -M xxx@gmail.com
    #PBS -l qos=debugging

    # modules
    module load Java/1.7.0_51
    module load Python/2.7.6-foss-2014a

    # environment
    export PBS_O_WORKDIR= /data/leuven/306/vsc30655/hadoop-test/hadoop-cf-14.10.23-11.06
    mkdir -p  /data/leuven/306/vsc30655/hadoop-test/hadoop-cf-14.10.23-11.06
    cd  /data/leuven/306/vsc30655/hadoop-test/hadoop-cf-14.10.23-11.06

    # execute scripts
    /vsc-mounts/leuven-data/306/vsc30655/mapredpyenv/bin/python -m hadoop2.bootstrap_hadoop2


This script is copied to the working directory and automatically submitted to the job system.

Example interaction:

    $ python hadoop-setup.py
    Job directory: /data/leuven/306/vsc30655/hadoop-test
    Number of nodes: 2
    Time (minutes): 2
    creating PBS script ... 
    Job script is stored in: /data/leuven/306/vsc30655/hadoop-test/hadoop-cf-14.11.24-13.59/work/job.pbs
    submitting job...
    job submitted with id:  20084117
    Waiting for node allocation
    



## Wait for Node Allocation

After submitting the PBS-script, the setup script will wait until the nodes are allocated. The generated output will resemble:

    Waiting for node allocation
    Esitmated start time:  00:00:01 on Mon Nov 24 13:59:04
    Esitmated start time:  00:00:27 on Mon Nov 24 13:59:04
    Esitmated start time:  00:00:00 on Mon Nov 24 13:59:37
    Esitmated start time:  00:00:23 on Mon Nov 24 13:59:37
    
This will provide an estimate on when the requested nodes will be available. It is possible that after the node allocation it still takes some time (minutes) before this is reflected here. The scripts waits for a file called `work/allocated` inside the working directory.

When the nodes are allocated, the Hadoop cluster itself will start to initialize.


## Waiting for Hadoop Initialization

Once the cluster nodes have been allocated, hadoop is initializing using a bootstrap script. After a few minutes, the cluster will be ready. The scripts provides a basic progress indication:

    Initializing Hadoop cluster
    . . . . . ' . . . . . | . . . . . ' .
    
The output will resemble the following:

    Hadoop clusted initialized (wait a few minutes to make sure ...).
    Allocated nodes:
    r3i0n1
    r5i1n15

    Use the following commands to ...
    ssh r5i1n15
    cd /data/leuven/306/vsc30655/hadoop-test/hadoop-cf-14.11.24-13.59
    bash ./hadoop_envir.sh

The information procided here are the allocated nodes, together with a set of commands to start using the hadoop cluster. The user should :

- log into the master node using ssh,
- navigate to the working directory, and
- setup the environment.

The last commands sets the environment variables and performes a smoke test. It will show the amount of storage available for DFS. The command that executes this smoke test is:

    hadoop dfsadmin -report

Variables that are set/modified in the envir file:

- HADOOP_CONF_DIR
- HADOOP_HOME
- PATH

This means you can run commands such as `hadoop` directly in the console. Also, other applications such as Pig can use the variables to find the current Hadoop installation.

Aside from this, a user directory is created in hdfs.

### Inner Workings of the Bootstrap Script
In the background, the hadoop bootstrap script creates one master node using a hadoop distribution. This master node is then configured and started. The master node itself then spawn the slave nodes. When the master node is set up, the bootstrap script creates a file `work/started` inside the working directory so that the setup-script knows the cluster is set up and is now initializing. 

The hadoop bootstrap script creates the following directory structure

    - work
        - hadoop
        - hadoop.tar.gz (downloaded file)
        - allocated (empty file, indicated that nodes have been allocated)
        - running (empty file, indicates that Hadoop cluster is started)
        - hadoop-conf-$jobid
            - etc
                - hadoop
                    - slaves
                    - masters
                    - core-site.xml
                    - hdfs-site.xml
                    - mapred-site.xml
                    - yarn-site.xml
                    - hadoop-env.sh (contains a modified JAVA_HOME)
            - name
            - logs



If no configuration is found in the environment variable then the bootstrap script creates a new hadoop configuration as follows:
<!-- @TODO (how can this be done? pbs file?) -->

- default config is copied from the hadoop installation
- the allocated nodes are listed
- master and slave files are created with the node ids
    - master contains nothing, only for secondary namenode!
    - slaves are all nodes
- xml configuration files are created
- current JAVA_HOME environment variable is inserted into hadoop-env.sh

Next, the folllowing actions are performed:

- environment vars are set correctly for child processes
- namenode is formatted if it is new (in our case it is always new)
- DFS is started
- YARN is started


TODO:
- where does the output of the bootstrap script go? -> in the error file of the job dir
- where does the script execute: on a cluster machine, with working dir on data server
- what are all these set_env() calls?
- why the export conf dir?


## Connect to the Cluster
After the environment script has been executed it is possible to execute hadoop commands. 

To be able to view the web-interface that is offered by different components of Hadoop, you can use port forwarding. A browser van be opened on localhost:50070 and localhost:8088 after issuing the following commands:
    
    NODE=r1i1n1
    ssh vsc30655@login.hpc.kuleuven.be -L 50070:$NODE:50070 -L 8088:$NODE:8088 -L 19888:$NODE:19888 -L 50075:$NODE:50075

Here `r1i1n1` is the hostname of the masternode.

## Test Cluster Using TeraSort

One way of testing the cluster is using the TeraSort distributed sort program.
First we generate some data `teragen`:

    hadoop jar work/hadoop-2.2.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.2.0.jar teragen 100000000 /user/vsc30655/teragen1

Next we sort it using `terasort`:

    hadoop jar work/hadoop-2.2.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.2.0.jar terasort /user/vsc30655/teragen1 /user/vsc30655/teraout2



## Generating Data

Data scripts are found in the hadoop-data directory 

### Q4
    
    sh generate_q4_hdfs.sh

### Generic
After setting up the environment, data needs to be loaded onto the cluster or generated.

We provide a script to generate the data directly on hdfs:

    generate_hdfs_data.sh num_rows num_chunks filename.py functionname hdfs_tmpdir hdfs_outdir
    
This script will use the function `functionname(rownr)` in the file filename.py, e.g.:

    def functionname(i):
        return i *  4 + 1


To test your generation function, you can use the generate_data.sh script:

    generate_data.sh




## Running a Pig Query

Once the environment is set up, and you have Pig in your path (in your ~/.bashrc file), Pig should be able to detect the Hadoop installation. This means now you can run Pig scripts on the cluster!

example script:

    R =  LOAD 'data/q4/1e06/R_6e06x4e00_func-seqclone.csv' as (x2,x3,x4,x5);
    S2 = LOAD 'data/q4/1e06/S2_3e06x1e00_func-non_mod_2.csv' as (x2);
    S3 = LOAD 'data/q4/1e06/S3_4e06x1e00_func-non_mod_3.csv' as (x3);
    S4 = LOAD 'data/q4/1e06/S4_4e06x1e00_func-non_mod_4.csv' as (x4);
    S5 = LOAD 'data/q4/1e06/S5_5e06x1e00_func-non_mod_5.csv' as (x5);
 
    S2_0_A = COGROUP R BY (x2), S2 BY (x2);
    S2_0_B = FILTER S2_0_A BY (IsEmpty(S2));
    Out1 = FOREACH S2_0_B GENERATE flatten(R);
 
    S3_0_A = COGROUP Out1 BY (x3), S3 BY (x3);
    S3_0_B = FILTER S3_0_A BY (IsEmpty(S3));
    Out2 = FOREACH S3_0_B GENERATE flatten(Out1);
 
    S4_0_A = COGROUP Out2 BY (x4), S4 BY (x4);
    S4_0_B = FILTER S4_0_A BY (IsEmpty(S4));
    Out3 = FOREACH S4_0_B GENERATE flatten(Out2);
 
    S5_0_A = COGROUP Out3 BY (x5), S5 BY (x5);
    S5_0_B = FILTER S5_0_A BY (IsEmpty(S5));
    Out4 = FOREACH S5_0_B GENERATE flatten(Out3);
 
    Out = Out4;

    STORE Out INTO 'output/Experiment_q4/pig3';



## Ready-to-go Queries
Some scripts can be found in the queries directory
    
    - script1.pig : Pig query on q4 data
    







# Appendix


## Imperfections
- java home is copied from user
- directory for dfs data is hardcoded
- if job history server is not started, then use the following:

    $HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start 
    
    this is now done by the bootstrapper...


## Cluster Settings:

Number of nodes: 176 with 64G RAM, 32 with 128
Number of cores per nodes: 20
Number of HDs per node: ??

Following Hortonworks manual:
Total available RAM =  64 - 8 = 56G (8 for system)
Minimum container size = 2048MB

number containers 
= min (2*CORES, 1.8*DISKS, (Total available RAM) / MIN_CONTAINER_SIZE)
= min (40, ??,  28)
= 20 (?)

Ram per container
= max(MIN_CONTAINER_SIZE, (Total Available RAM) / containers))
= max(2048MB, 2867,2)
= 2867,2
~ 2816 (= 2048 + 512 + 256)


### yarn-site.xml	

- yarn.nodemanager.resource.memory-mb	= containers * RAM-per-container = 20 * 2816 = 56320
- yarn.scheduler.minimum-allocation-mb	= RAM-per-container = 2816
- yarn.scheduler.maximum-allocation-mb	= containers * RAM-per-container = 20 * 2816 = 56320

### mapred-site.xml	

- mapreduce.map.memory.mb	= RAM-per-container = 2816
- mapreduce.reduce.memory.mb	= 2 * RAM-per-container = 5632
- mapreduce.map.java.opts	= 0.8 * RAM-per-container = 2252
- mapreduce.reduce.java.opts	= 0.8 * 2 * RAM-per-container = 4505
- (check) yarn.app.mapreduce.am.resource.mb	= 2 * RAM-per-container = 2 * 2816 = 5632
- (check) yarn.app.mapreduce.am.command-opts	= 0.8 * 2 * RAM-per-container = 0.8 * 5632 = 4505





