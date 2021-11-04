# CS4224 Cassandra

## Project Setup

#### 1. Install Cassandra

Please refer to the installation instructions from Cassandra docs:
http://cassandra.apache.org/doc/latest/cassandra/getting_started/installing.html

#### 2. Install Python dependencies

You may use either pipenv or manual installation to install the python dependencies.

a) Using pipenv
```
pip install pipenv
pipenv install
pipenv shell
```

b) Manual installation
```
pip install cassandra-driver
```

#### 3. Download data from CS4224 website

The data files are not included in this repository due to their large size. You may either download and extract them manually, or run the following commands in the project's root.

```bash
wget http://www.comp.nus.edu.sg/~cs4224/project_files_4.zip
unzip project_files_4.zip
```

Now, you should see a folder called `project_files_4` in the project's root folder.



## Configuration details

### Modifications to conf/cassandra.yaml

#### 1. Configure setup

```
Cluster name: "CS4224B Cluster"	
listen_address: 192.168.48.169 (replace this with node ip)
rpc_address: 192.168.48.169 (replace this with node ip)
seeds: "192.168.48.169,192.168.48.170" 
native_transport_port: 6042
storage_port: 6000
ssl_storage_port: 6001
``` 

#### 2. Configure timeout

```
read_request_timeout_in_ms: 120000 
range_request_timeout_in_ms: 400000
write_request_timeout_in_ms: 50000
request_timeout_in_ms: 400000 
```

### Modifications to bin/cqlsh.py

Set the default consistency level to LOCAL_QUORUM

```
self.consistency_level = cassandra.ConsistencyLevel.LOCAL_QUORUM
```

## Running the experiments


#### 1. Install sshpass if you don't have it installed on your machine.
Then, ssh to each machine from xcnc20 to xcnc24

```
sshpass -p <cluster_password> ssh cs4224b@xcnc20.comp.nus.edu.sg
```


#### 2. Start Cassandra on each machine

```
cassandra
```

#### 3. Set up the datacenter

```
cd temp/CS4224_Cassandra
python3 create_db_tables.py
python3 clean_data.py
cqlsh 192.168.48.169 6042 -f create_index.cql
cqlsh 192.168.48.169 6042 -f load_db_data.cql
```

#### 4. In your terminal, run the experiment

```
bash run_experiment.sh <cluster_password> <experiment_type>
```
experiment_type should either be A or B

#### 5. Upon completion of experiment, compute the statistics

```
python3 consolidate_client_results.py
python3 compute_throughput_statistics.py
python3 compute_end_state_statistics.py
```

## Useful commands

#### Check cassandra's logs

```
tail -f ~/temp/apache-cassandra-4.0.1/logs/system.log
```

#### Check status of nodes

```
nodetool status
```

#### Check logs of experiment from your terminal

```
tail -f nohup.out
```

#### Stop existing cassandra processes

```
ps ax | grep cassandra
kill <process id>
```