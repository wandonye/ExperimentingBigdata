# Experimenting Bigdata


## Setup

### VMs configuration

Use 5 VMs hosted in CloudLab. One VM has a public IP which is accesible from the external world.  All VMs have a private IP and are reachable from each other. Only the VM has a public IP while the others are assigned as slaves only.  In the following, when we refer to a master, we refer to either the VM designated as the master, or to a process running on the master VM.

```
Installed the following packages on every VM
sudo apt-get update --fix-missing.
sudo apt-get install openjdk-7-jdk.
sudo apt-get install pdsh (parallel distributed shell).
```

Created a hierarchy of directories required during the software deployment step. This hierarchy should exists on all the VMs and it is required by various framework specific configurations. For example, /home/ubuntu/storage/hdfs/hdfs_dn_dirs is required by dfs.datanode.data.dir property in hdfs-site.xml and it determines where on the local filesystem an HDFS data node should store its blocks. /home/ubuntu/logs/apps is required by yarn.nodemanager.log-dirs to store container logs. /home/ubuntu/storage/data/spark/rdds_shuffle is required to store shuffle data and RDDs that get spilled to disk. Spark applications run in /home/ubuntu/storage/data/spark/worker. Other structures such as /home/ubuntu/conf, logs, software, storage, workload are mainly designed for convenience and easier debugging in case you face issues that require further investigation.

```
/home/ubuntu/conf, logs, software, storage, workload.
/home/ubuntu/logs/apps, hadoop.
/home/ubuntu/storage/data/local/nm, tmp.
/home/ubuntu/storage/hdfs/hdfs_dn_dirs, hdfs_nn_dir.
/home/ubuntu/workload.
/home/ubuntu/storage/data/spark/rdds_shuffle.
/home/ubuntu/logs/spark.
/home/ubuntu/storage/data/spark/worker.
```

Updated/etc/hosts with information about all VM Private IPs and their hostnames. This file should be updated on every VM in your setup.

Before proceeding, please ensure all the above steps have been done on your VMs.

### Software deployment

Deploy all the software needed and set the proper configurations following these steps below:

Download the following configuration archive on every VM, and place the uncompressed files in /home/ubuntu/conf.  In addition, download and deploy the run.sh script in your home directory (/home/ubuntu) on every VM. The script enables you to start and stop Hadoop services and configure various environment variables. To function properly, it requires an machines file which contains the IP addresses of all the slave VMs where you plan to deploy your big data software stack, one per line. For example, if you plan to deploy slave processes on VMs with IPs 10.0.1.2, 10.0.1.3, 10.0.1.4, your machines file will look like:
```
10.0.1.2
10.0.1.3
10.0.1.4
```
The run.sh script used in assignment-1 incorporates additional information required to run the SPARK stack. It defines new environment variables and new commands to start and stop your SPARK cluster.
```
SPARK_HOME=/home/ubuntu/software/spark-2.0.0-bin-hadoop2.6. // specify the home directory for your SPARK installation
SPARK_CONF_DIR=/home/ubuntu/conf. // specify the location of the configuration files required by SPARK
SPARK_MASTER_IP=ip_master (e.g. 10.0.1.13). // specify the IP of the Master
SPARK_LOCAL_DIRS=/home/ubuntu/storage/data/spark/rdds_shuffle. // specify the directory to store RDDs and shuffle input
SPARK_LOG_DIR=/home/ubuntu/logs/spark. // specify the directory to store spark process logs
SPARK_WORKER_DIR=/home/ubuntu/logs/apps_spark. // application logs directory
```
Be sure to replaace the SPARK_MASTER_IP and SPARK_MASTER_HOST with the private IP of your master VM in run.sh.

Next, we will go through deploying Hadoop and Hive (as done in the previous assignment), the Spark stack as well as the Storm stack. The following figure describes the Spark software stack you will deploy:

### Apache Hadoop
The first step in building your big data stack is to deploy Hadoop. Hadoop mainly consists of two parts: a distributed filesystem called HDFS and a resource management framework known as YARN. As you will learn in the class, HDFS is responsible for reliable management of the data across the distributed system, while YARN handles the allocation of available resources to existing applications.

To become familiar with the terminology, HDFS consists of a master process running on the master instance, called NameNode and a set of slave processes one running on each slave instance called DataNode. The NameNode records metadata information about every block of data in HDFS, it maintains the status of the DataNodes, and handles the queries from clients. A DataNode manages the actual data residing on the corresponding instance. Similarly, in YARN there is a master process known as ResourceManager which maintains information about the available instances/resources in the system and handles applications resource requests. A NodeManager process is running on every instance and manages the available resources, by periodically "heartbeating" to the ResourceManager to update its status.

Now that you know the terminology, download hadoop-2.6.0.tar.gz. Deploy the archive in the /home/ubuntu/software directory on every VM and untar it (tar -xzvf hadoop-2.6.0.tar.gz).

You should modify core-site.xml, hdfs-site.xml, yarn-site.xml, mapred-site.xml in your /home/ubuntu/conf directory on every VM and replace the master_ip with the private IP of your master VM.

Before launching the Hadoop daemons, you should set the proper environment variables from the script run.sh and format the Hadoop filesystem. To enable the environment variables you need to run the command: source run.sh. To format the filesystem (which simply initializes the directory specified by the dfs.namenode.name.dir) run the command: `hadoop namenode -format`.

Finally, you can instantiate the Hadoop daemons by running start_all in your command line on the master VM. To check that the cluster is up and running you can type jps on every VM. Each VM should run the NodeManager and DataNode processes. In addition, the master VM should run the ResourceManager, NodeManager, JobHistoryServer, ApplicationHistoryServer, Namenode, DataNode. The processesJobHistoryServer and ApplicationHistoryServer are history servers designed to persist various log files for completed jobs in HDFS. Later on, you will need these logs to extract various information to answer questions in your assignment. In addition, you can check the status of your cluster, at the following URLs:

```
http://public_ip_master:50070 for HDFS.
http://public_ip_master:8088 for YARN.
http://public_ip_master:19888 for M/R job history server.
http://public_ip_master:8188 for application history server.
```

To test that your Hadoop cluster is running properly, you can run a simple MapReduce application by typing the following command on the master VM:
```
hadoop jar software/hadoop-2.6.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.0.jar pi 1 2.
```
### Apache Hive
Hive is a data warehouse infrastructure which provides data summarization, query, and analysis. To deploy Hive, you also need to deploy a metastore service which stores the metadata for tables and partitions in a relational database, and provides Hive access to this information via the metastore service API.

To deploy the metastore service you need to do the following:

Install mysql-server which will be used to store the metastore information. On your master VM you should run the command: sudo apt-get install mysql-server.

After you have installed the mysql-server, you should replace the bind-address in /etc/mysql/my.cnf to point to your eth0 IP address. Finally, restart the mysql server: sudo /etc/init.d/mysql restart.

Login to your mysql console as root: mysql -u root -p .

After you are logged in, create a metastore database in your mysql console: mysql> create database metastore;.
Select the newly created metastore database: mysql> use metastore;.

Create a new user hive identified by password hive :mysql> create user 'hive'@'IP_master' identified by 'hive';.

Grant privileges on the metastore database to the newly created user: mysql> grant all privileges on metastore.* to 'hive'@'IP_master' identified by 'hive'; .
```
Flush privileges mysql> flush privileges; .
mysql> quit; .
```
You should modify hive-site.xml in your /home/ubuntu/conf directory on the master VM and replace the master_ip with it's IP.

Finally, download and untar hive-1.2.1.tar.gz in /home/ubuntu/software on the master VM. If you type hive in the command line, you should be able to enter hive console >hive.

### Apache Spark
You will use HDFS as the underlying filesystem.

To become familiar with the terminology, SPARK standalone consists of a set of daemons: a Master daemon, which is the equivalent of the ResourceManager in YARN terminology, and a set of Worker daemons, equivalent to the NodeManager processes. SPARK applications are coordinated by a SparkContext object which will connect to the Master, responsible for allocating resources across applications. Once connected, SPARK acquires Executors on every Worker node in the cluster, which are processes that run computations and store data for your applications. Finally, the application's tasks are handled to Executors for execution. You can read further about the SPARK architecture here.

Now that you are familiar with the SPARK terminology, download spark-2.0.0-bin-hadoop2.6.tgz . Deploy the archive in the /home/ubuntu/software directory on every VM and untar it (tar -xzvf spark-2.0.0-bin-hadoop2.6.tgz).

You should modify hive-site.xml and set the hive.metastore.uris property to thrift://ip_master:9083.

Finally, you can instantiate the SPARK daemons by running start_spark on your Master VM. To check that the cluster is up and running you can check that a Master process is running on your master VM, and a Worker is running on each of your slave VMs. In addition you can check the status of your SPARK cluster, at the following URLs:

http://public_ip_master:8080 for SPARK.

http://public_ip_master:4040 for your application context. Note that this will be available only when an application is running.

### Apache Zookeeper

Apache Storm has its own stack, and it requires Apache Zookeeper for coordination in cluster mode. To install Apache Zookeeper you should follow the instructions here. First, you need to download zookeeper-3.4.6.tar.gz, and deploy it on every VM in /home/ubuntu/software. Next, you need to create a zoo.cfg file in /home/ubuntu/software/zookeeper-3.4.6/conf directory. You can use zoo.cfg as a template. If you do so, you need to replace the VMx_IP entries with your corresponding VMs IPs and create the required directories specified by dataDir and dataLogDir. Finally, you need to create a myid file in your dataDir and it should contain the id of every VM as following: server.1=VM1_IP:2888:3888, it means that VM1 myid's file should contain number 1.

Next, you can start Zookeeper by running zkServer.sh start on every VM. You should see a QuorumPeerMain process running on every VM. Also, if you execute zkServer.sh status on every VM, one of the VMs should be a leader, while the others are followers.
### Apache Storm
A Storm cluster is similar to a Hadoop cluster. However, in Storm you run topologies instead of "MapReduce" type jobs, which eventually processes messages forever (or until you kill it). To get familiar with the terminology and various entities involved in a Storm cluster please read here.

To deploy a Storm cluster, please read here. For this assignment, you should use apache-storm-1.0.2.tar.gz. You can use this file as a template for the storm.yaml file required. To start a Storm cluster, you need to start a nimbus process on the master VM (storm nimbus), and a supervisor process on any slave VM (storm supervisor). You may also want to start the UI to check the status of your cluster (PUBLIC_IP:8080) and the topologies which are running (storm ui).
