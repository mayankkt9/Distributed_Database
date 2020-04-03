sudo -u hduser hadoopdir/bin/hadoop jar equijoin.jar equijoin hdfs://localhost:54310/input/sample.txt hdfs://localhost:54310/output


sudo hadoopdir/bin/hadoop jar equijoin.jar equijoin hdfs://localhost:54310/input/sample.txt hdfs://localhost:54310/output


javac -cp `hadoopdir/bin/hadoop classpath` eclipse_workspace_dds/DDS/src/map_reduce/equijoin.java -d eclipse_workspace_dds/DDS/build/

rm equijoin.jar
javac -cp `hadoopdir/bin/hadoop classpath` eclipse_workspace_dds/DDS/src/equijoin.java -d eclipse_workspace_dds/DDS/build/
cd eclipse_workspace_dds/DDS/build/
jar -cvf equijoin.jar .
cd -
mv eclipse_workspace_dds/DDS/build/equijoin.jar .

sudo -u mayankkataruka hadoopdir/bin/hadoop jar EquiJoin.jar  equijoin datadir/sample.txt output7


cd /Users/mayankkataruka/Desktop/Work/ASU_STUDY/2ndSem/Distributed_Database/DDS_Workspace

hadoopdir/sbin/stop-dfs.sh 
echo Y | hadoopdir/bin/hadoop namenode -format
hadoopdir/sbin/start-dfs.sh 



rm -rf /tmp/hadoop-mayankkataruka*
rm -rf datadir/current/
rm -rf namenode/*
rm -rf datanode/*
rm -rf /Users/mayankkataruka/Desktop/Work/ASU_STUDY/2ndSem/Distributed_Database/DDS_Workspace/tmp/*
rm -rf hadoopdir/logs/*



sudo lsof -i tcp:50070


hadoopdir/bin/hadoop fs -mkdir -p /user/mayankkataruka/datadir
hadoopdir/bin/hadoop fs -put datadir/input/sample.txt /user/mayankkataruka/datadir/



