package java_kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;

public class consumer {

	public static void main(String[] args) {
		new consumer("localhost:9092",  "user_registered").run();
	}

	private final Logger mLogger = LoggerFactory.getLogger(consumer.class.getName());
	private String mBootstrapServer = "";

	private String mTopic = "";

	consumer(String bootstrapServer,  String topic) {
		mBootstrapServer = bootstrapServer;
		mTopic = topic;
	}

	void run() {
		mLogger.info("HILO CONSUMIDOR -----");
		CountDownLatch latch = new CountDownLatch(1);

		ConsumerRunnable consumerRunnable = new ConsumerRunnable(mBootstrapServer, mTopic, latch);
		Thread thread = new Thread(consumerRunnable);
		thread.start();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			mLogger.info("Caught shutdown hook");
			consumerRunnable.shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			mLogger.info("----------------SALIENDO DE LA APLICACIÓN");
		}));

		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private class ConsumerRunnable implements Runnable {
		private CountDownLatch mLatch;
		private KafkaConsumer<String, String> mConsumer;
		
		ConsumerRunnable(String bootstrapServer, String topic, CountDownLatch latch) {
			mLatch = latch;

			Properties props = consumerProps(bootstrapServer);
			mConsumer = new KafkaConsumer<>(props);
			mConsumer.subscribe(Collections.singletonList(topic));
		}
		private Properties consumerProps(String bootstrapServer ) {
			String deserializer = StringDeserializer.class.getName();
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, mTopic);

			return properties;
		}

		@Override
		public void run() {
			try {
				do {
					System.out.println ("escuchando.....");
					ConsumerRecords<String, String> records = mConsumer.poll(Duration.ofMillis(1000));

					for (ConsumerRecord<String, String> record : records) {
						mLogger.info("Key: " + record.key() + ", Value: " + record.value());
						mLogger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
					}
				} while (true);
			} catch (WakeupException e) {
				mLogger.info("------------------APAGANDO CONSUMIDOR!");
			} finally {
				mConsumer.close();
				mLatch.countDown();
			}
		}

		void shutdown() {
			mConsumer.wakeup();
		}
	}

}
