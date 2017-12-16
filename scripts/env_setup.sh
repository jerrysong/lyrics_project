PATH=$PATH:$HOME/.local/bin:$HOME/bin

export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.151-1.b12.el7_4.x86_64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_INSTALL=/usr/local/hadoop
export HADOOP_CONF_DIR=$HADOOP_INSTALL/etc/hadoop
export YARN_HOME=$HADOOP_HOME
export YARN_CONF_DIR=$YARN_HOME/etc/hadoop
export MAVEN_HOME=/usr/local/maven
export HBASE_HOME=/usr/local/hbase
export SPARK_HOME=/usr/local/spark
export NGINX_HOME=/usr/local/nginx
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$MAVEN_HOME/bin:$HBASE_HOME/bin:$SPARK_HOME/sbin:$NGINX_HOME/sbin
export PROD=/root/lyrics_project
export PYTHONPATH=$PYTHONPATH:$PROD/common
