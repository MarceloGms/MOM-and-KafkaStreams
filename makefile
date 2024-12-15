compile:
	@mvn clean install

routes:
	@mvn exec:java -Dexec.mainClass="Routes"

#kafka-console-consumer.sh --bootstrap-server broker1:9092 --topic Routes --from-beginning

trips:
	@mvn exec:java -Dexec.mainClass="PassengerTrips"

#kafka-console-consumer.sh --bootstrap-server broker1:9092 --topic Trips --from-beginning

streams:
	@mvn exec:java -Dexec.mainClass="KafkaStreamsApp"

server:
	@mvn spring-boot:run