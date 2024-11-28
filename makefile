compile:
	@mvn clean package

producer:
	@mvn exec:java -Dexec.mainClass="ProducerDemo"

consumer:
	@mvn exec:java -Dexec.mainClass="ConsumerDemo"

fraud:
	@mvn exec:java -Dexec.mainClass="FraudDetectionApp"

word:
	@mvn exec:java -Dexec.mainClass="WordCountDemo"

streams:
	@mvn exec:java -Dexec.mainClass="SimpleStreamsExercises" -Dexec.args="kstreamstopic"

prod:
	@mvn exec:java -Dexec.mainClass="SimpleProducer" -Dexec.args="kstreamstopic"