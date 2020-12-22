# Simple flink demo with a python job


### Download flink 1.12 and untar it in ~/flink-1.12.0
```shell
https_proxy=https://webproxy.eqiad.wmnet:8080/ wget (choose a mirror from https://www.apache.org/dyn/closer.lua/flink/flink-1.12.0/flink-1.12.0-bin-scala_2.11.tgz) -O ~/flink-1.12.0-bin-scala_2.11.tgz
tar xvzf ~/flink-1.12.0-bin-scala_2.11.tgz -C ~/
FLINK_HOME=~/flink-1.12.0
# download the kafka connector
https_proxy=https://webproxy.eqiad.wmnet:8080/ wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka_2.11/1.12.0/flink-connector-kafka_2.11-1.12.0.jar -O $FLINK_HOME/lib/flink-connector-kafka_2.11-1.12.0.jar
https_proxy=https://webproxy.eqiad.wmnet:8080/ wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.4.1/kafka-clients-2.4.1.jar -O $FLINK_HOME/lib/kafka-clients-2.4.1.jar
```


### Create a virtual env
```shell
python3 -m venv $FLINK_HOME/venv
source venv/bin/activate
pip --proxy=https://webproxy.eqiad.wmnet:8080/ install wheel
pip --proxy=https://webproxy.eqiad.wmnet:8080/ install "apache-flink==1.12.0"
(cd $FLINK_HOME && zip -r venv.zip venv)
```

### Run the yarn session cluster
```shell
# open your kerberos session
# run the yarn session cluster
cd $FLINK_HOME
HADOOP_CLASSPATH="`hadoop classpath`" bin/yarn-session.sh -nm "My flink experiment"
# note the last log message it should indicate which address to connect to to access the flink UI and open tunnel to it e.g.:
# ssh -L 8888:an-worker1103.eqiad.wmnet:42409 stat1004.eqiad.wmnet

##Run the job
cd $FLINK_HOME
HADOOP_CLASSPATH="`hadoop classpath`" ./bin/flink run --python ../api_actions.py --pyarch venv.zip -pyexec venv.zip/bin/python
