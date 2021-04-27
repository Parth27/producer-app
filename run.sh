mvn package;
mvn exec:java -Dexec.mainClass=com.kafka.producer.App -Dexec.args="4567"