compile:
	@mvn clean install

routes:
	@mvn exec:java -Dexec.mainClass="tp3.kafka.Routes"

#kafka-console-consumer.sh --bootstrap-server broker1:9092 --topic Routes --from-beginning

trips:
	@mvn exec:java -Dexec.mainClass="tp3.kafka.PassengerTrips"

#kafka-console-consumer.sh --bootstrap-server broker1:9092 --topic Trips --from-beginning

streams:
	@mvn exec:java -Dexec.mainClass="tp3.kafka.KafkaStreamsApp"

server:
	@mvn spring-boot:run