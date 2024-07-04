#! /bin/sh
mvn clean package -Dname=$1 && \
sshpass -p <PASSWORD> scp target/$1-1.0-SNAPSHOT.jar hadoop@10.1.1.58:/home/hadoop/LetterFrequency/$1-1.0-SNAPSHOT.jar && \
ts=$(date +%y%m%d-%H%M%S) && \
sshpass -p <PASSWORD> ssh hadoop@10.1.1.58 "echo \"\$(/usr/bin/time -o logs/$1-$2 /opt/hadoop/bin/hadoop jar /home/hadoop/LetterFrequency/$1-1.0-SNAPSHOT.jar it.unipi.hadoop.$1 inputLetterFrequency/$2 outputLetterFrequency/$2-$ts $3 2>&1 )\" | tee -a logs/$1-$2" && \
sshpass -p <PASSWORD> ssh hadoop@10.1.1.58 "/opt/hadoop/bin/hadoop fs -cat outputLetterFrequency/$2-$ts/part* "
