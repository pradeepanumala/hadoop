#!/bin/bash
java -cp /home/pradeep/workspace/gossip/protobuf-java-2.5.0.jar:bin namenode.NameNodeServer /home/pradeep/hdfs/configFiles/ &
sleep 5
java -cp /home/pradeep/workspace/gossip/protobuf-java-2.5.0.jar:bin datanode.DataNodeServer /home/pradeep/hdfs/configFiles/ &
sleep 2
java -cp /home/pradeep/workspace/gossip/protobuf-java-2.5.0.jar:bin namenode.JobTracker
sleep 2 &
java -cp /home/pradeep/workspace/gossip/protobuf-java-2.5.0.jar:bin taskTracker.TaskTracker &
