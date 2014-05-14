#! /usr/bin/env bash
JARFILE=target/hbase-messenger-0.0.1-jar-with-dependencies.jar
java -jar $JARFILE "$@"
