package com.kafka.elasticsearch;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class KafkaElasticSearchConsumer {

	private Logger logger = LoggerFactory.getLogger(KafkaElasticSearchConsumer.class);
	
	public KafkaElasticSearchConsumer() {}
	
	public static void main(String[] args) throws IOException {
//		new KafkaElasticSearchConsumer().run();
		new KafkaElasticSearchConsumer().bulkRun();
	}

	/*
	 * This method handles requests one by one
	 */
	public void run() throws IOException {
		String topic = "twitter";
		
		//create elasticsearch high level client
		RestHighLevelClient client = createElasticSearchHighLevelClient();						
		
		//Create Kafka Consumer and subscribe to twitter topics
		try (KafkaConsumer<String, String> consumer = createConsumer(topic)) {

			IndexRequest request;
			IndexResponse response;
			
			while (true) {
				// Poll for Consumer records
				ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
				logger.info("Received " + consumerRecords.count() + " records in this poll on "+ consumerRecords.partitions().toString());
				for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					//logger.info("key: " + consumerRecord.key() + ", value: " + consumerRecord.value());
					//logger.info("partition: " + consumerRecord.partition() + ", offset: " + consumerRecord.offset());
					/*
					 * We need to make consumer idempotent, means there should not be any duplicates inserted into elastic search
					 * 
					 * To implement that, an unique identifier needs to added into index request. So that, if the same message inserted again, it will update the existing message instead of inserting again
					 * 
					 * The unique id can be generated in 2 ways,
					 * 
					 * The kafka generic one is, String id = consumerRecord.topic() + "_" + consumerRecord.partition() + "_" + consumerRecord.offset(); // This will be duplicated
					 * 
					 * Other way is to use any identifier present in the json source string. Here in this example, twitter's tweet has a unique id tag 'id_str' which we are going to use in this example
					 */
					
					String id = extractUniqueIdFromTweet(consumerRecord.value());
					
					//put consumer record's value into elastic search
					request = new IndexRequest("twitter", "tweets", id).source(consumerRecord.value(), XContentType.JSON);

					response = client.index(request, RequestOptions.DEFAULT);

					logger.info(response.getId());
					
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

				}
				logger.info("Commiting offsets");
				consumer.commitSync();
				logger.info("Offsets have been commited");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	/*
	 * This method handles bulk requests to elastic search
	 */
	public void bulkRun() throws IOException {
		String topic = "twitter";
		
		//create elasticsearch high level client
		RestHighLevelClient client = createElasticSearchHighLevelClient();						
		
		//Create Kafka Consumer and subscribe to twitter topics
		try (KafkaConsumer<String, String> consumer = createConsumer(topic)) {

			
			BulkRequest request = new BulkRequest();
									
			IndexResponse response;
			
			while (true) {
				// Poll for Consumer records
				ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
				logger.info("Received " + consumerRecords.count() + " records in this poll on "+ consumerRecords.partitions().toString());
				for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					//logger.info("key: " + consumerRecord.key() + ", value: " + consumerRecord.value());
					//logger.info("partition: " + consumerRecord.partition() + ", offset: " + consumerRecord.offset());
					/*
					 * We need to make consumer idempotent, means there should not be any duplicates inserted into elastic search
					 * 
					 * To implement that, an unique identifier needs to added into index request. So that, if the same message inserted again, it will update the existing message instead of inserting again
					 * 
					 * The unique id can be generated in 2 ways,
					 * 
					 * The kafka generic one is, String id = consumerRecord.topic() + "_" + consumerRecord.partition() + "_" + consumerRecord.offset(); // This will be duplicated
					 * 
					 * Other way is to use any identifier present in the json source string. Here in this example, twitter's tweet has a unique id tag 'id_str' which we are going to use in this example
					 */
					
					String id = extractUniqueIdFromTweet(consumerRecord.value());
					
					//put consumer record's value into elastic search
					request.add(new IndexRequest("twitter", "tweets", id).source(consumerRecord.value(), XContentType.JSON));

				}
				
				if (Integer.valueOf(consumerRecords.count()) > 1) {
					BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
					logger.info(bulkResponse.getItems().toString());
					logger.info("Commiting offsets");
					consumer.commitSync();
					logger.info("Offsets have been commited");
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					} 
				}
			}
		}
		
	}
	
	private static JsonParser parser = new JsonParser();
	
	private String extractUniqueIdFromTweet(String tweet) {
		// TODO Auto-generated method stub
		return parser.parse(tweet)
			.getAsJsonObject()
			.get("id_str")
			.getAsString();
	}

	public RestHighLevelClient createElasticSearchHighLevelClient() {
		/*
		 * Full Access URL from Bonsai Credentials page for the cluster created
		 * 		
		 * https://xtzrbx1ee3:anmi98bq4a@kafka-cluster-6122293213.us-east-1.bonsaisearch.net:443
		 * 
		 */
		String hostname = "kafka-cluster-6122293213.us-east-1.bonsaisearch.net";
		String username = "xtzrbx1ee3";
		String password = "anmi98bq4a";
		int port = 443;
		String protocol = "https";
		
		/*
		 * Build a credential provider for authorization
		 * 
		 * Here, we use basic username and password credentials provider; 
		 * 
		 */
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
		
		/*
		 * Host to connect to bonsai elasticsearch cluster
		 */
		HttpHost host = new HttpHost(hostname, port, protocol);
		
		
		// Rest Client Builder for ElasticSearch Client
		RestClientBuilder restClientBuilder = RestClient.builder(host).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {			
			@Override
			public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
				return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
			}
		});
		     
		
		//Build the  High Level Rest Client
		RestHighLevelClient client = new RestHighLevelClient(
		        restClientBuilder);
		
		
		return client;		
	}
	
	public KafkaConsumer<String,String> createConsumer(String topic) {
		// Create Consumer Properties
		Properties consumerProps = new Properties();
		consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			//specify the consumer group it belongs to
		consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-elasticsearch-consumer-application");
			//it can be earliest/latest/none -> earliest: resets the offset to the earliest offset
		consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		//to set number of records per poll
		consumerProps.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "25");
		//to disable auto commit of messages when they polled
		consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		
		// Create Consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProps);

		//subscribe to topic
		consumer.subscribe(Arrays.asList(topic));	
		
		return consumer;
	}
}
