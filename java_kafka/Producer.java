package java_kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;

public class producer {
	
	KafkaProducer<String, String> mProducer = null;
	final org.slf4j.Logger mLogger = LoggerFactory.getLogger(producer.class);
	
	producer (String bootstrapServer) {
		Properties props = producerProps(bootstrapServer);
		mProducer = new KafkaProducer<String, String>(props);
		mLogger.info("Producer initialized");
		
	}
	
	private Properties producerProps(String bootstrapServer) {
		String serializer = StringSerializer.class.getName();
		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer);
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);
		
		return props;
	}
	
	void put(String topic, String key, String value) throws ExecutionException, InterruptedException {
		mLogger.info("Put value: " + value + ", for key " + key);
		
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
		mProducer.send(record, (recordMetadata, e) -> {
			if (e != null) {
				(mLogger).error("Error while producing", e);
				return;
			}
			
			mLogger.info("Received new meta. \n" + 
			"Topic: " + recordMetadata.topic() + "\n" + 
			"Partition: " + recordMetadata.partition() + "\n" + 
			"Offset: " + recordMetadata.offset() + "\n" + 
			"Timestamp: " + recordMetadata.timestamp());
		}).get();
	}
	
	void close() {
		mLogger.info("Closing producer's connection");
		mProducer.close();
	}
	

}
