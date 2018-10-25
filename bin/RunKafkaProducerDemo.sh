#!/usr/bin/env bash

JAVA=$JAVA_HOME/bin/java

KAFKA_HOME=/Users/hongtaozhang/workspace/kafka_2.11-0.11.0.2

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

target=$bin/../target/quickstart-0.1.jar

CLASSPATH=$target
# add directory to classPath
for file in "$KAFKA_HOME"/libs/*;
do
    CLASSPATH="$CLASSPATH":"$file"
done

echo $CLASSPATH

echo $@

$JAVA -cp $CLASSPATH org.infinivision.flink.examples.kafka.ProducerDemo $@
