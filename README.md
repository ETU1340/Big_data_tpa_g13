# Big_data_tpa_g13
Telecharger le dossier /ELT et placé le dans votre dossier vagrant 

### Demmarage de l'application java ELT
'''path 
export MYPATH=/vagrant/ELT/

-- compile application 
javac -cp $MYPATH/jar/*:$MYPATH  $MYPATH/Client.java

-- execute application 
java -cp $MYPATH/jar/*:$MYPATH  Client


## Hive


### Start Hive (Metastore service & HiveServer2)

> Note: Prior starting Hive, it is required that Hadoop services are running.
> See [Start Hadoop](#start-hadoop-hdfs--yarn)

```bash
nohup hive --service metastore > hive_metastore.log 2>&1 &
nohup hiveserver2 > hive_server.log 2>&1 &
```

### Connect to Hive

> Note: Prior connecting to Hive, make sure to wait for 1-3 minutes after
> launching HiveServer2 as the Hive service needs some time to become operational.

```bash
beeline -u jdbc:hive2://localhost:10000 vagrant
```
-- Query the table
SELECT * from client;
