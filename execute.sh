mvn package;
mvn dependency:copy-dependencies;
mvn exec:java -Dexec.mainClass=com.kafka.producer.App