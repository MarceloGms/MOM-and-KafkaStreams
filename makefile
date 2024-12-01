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
	@mvn exec:java -Dexec.mainClass="tp3.blog.SimpleStreamsExercises" -Dexec.args="kstreamstopic"

prod:
	@mvn exec:java -Dexec.mainClass="tp3.blog.SimpleProducer" -Dexec.args="kstreamstopic"

balanceproducer:
	@mvn exec:java -Dexec.mainClass="tp3.balance.TransactionProducer"

balancecalculator:
	@mvn exec:java -Dexec.mainClass="tp3.balance.BalanceCalculator"