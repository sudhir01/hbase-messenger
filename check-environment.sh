#! /usr/bin/env bash
JARFILE=target/hbase-messenger-0.0.1-jar-with-dependencies.jar
if ! [ -a $JARFILE ]; then 
echo "HBase-Messenger has not been compiled yet.";
command -v mvn >/dev/null 2>&1 || { echo "Maven is required to compile the plugin. Please install it correctly for your platform and run this script again." >&2; exit 1; } 
echo "Maven has been found. Attempting to compile hbase messenger.";
mvn package
echo "If build was successful, then assuming you are runing hbase 0.96 and zookeeper, you you should be able to interact with hbase-messenger by using the hbase-messenger.sh shell script.
else 
echo "$JARFILE has been found. Assuming you are running hbase 0.96 and zookeeper, you should be able to interact with hbase-messenger by using the hbase-messenger.sh shell script."
fi