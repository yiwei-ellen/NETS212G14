package edu.upenn.cis.nets212.hw3.livy;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
 
import scala.Tuple2;

public class ComputeRecLivy {
	public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ExecutionException {
		
		LivyClient client = new LivyClientBuilder()
				  .setURI(new URI("http://ec2-3-86-199-174.compute-1.amazonaws.com:8998/"))
				  .build();
		
		AmazonDynamoDB dbclient = AmazonDynamoDBClientBuilder.standard().withRegion("us-east-1").build();
		
		while(true) {
			try {
				// Create a new list of tuples with user ids of pairs of friends
				List<Tuple2<String, String>> friends = new ArrayList<Tuple2<String, String>>();
				ScanRequest scanRequest = new ScanRequest().withTableName("friends");
			
				// Scan through the friends table and convert all records into an RDD
				ScanResult res = dbclient.scan(scanRequest);
				for (Map<String, AttributeValue> item : res.getItems()){
					friends.add(new Tuple2(item.get("userid").getS(), item.get("useridfriend").getS()));
				}
			
				// Create a new list of tuples with user ids and interests
				List<Tuple2<String, String>> interests = new ArrayList<Tuple2<String, String>>();
				scanRequest = new ScanRequest().withTableName("users");
			
				// Scan through the users table and convert all records into an RDD
				res = dbclient.scan(scanRequest);
				for (Map<String, AttributeValue> item : res.getItems()){
					if (item.get("interests") != null) {
						for (String interest : item.get("interests").getSS()) {
							interests.add(new Tuple2(item.get("userid").getS(), interest));
						}
					}
				}
			
				// Create a new list of tuples with user ids and articles they liked
				List<Tuple2<String, String>> likes = new ArrayList<Tuple2<String, String>>();
				scanRequest = new ScanRequest().withTableName("newslikes");
					
				// Scan through the users table and convert all records into an RDD
				res = dbclient.scan(scanRequest);
				for (Map<String, AttributeValue> item : res.getItems()){
					likes.add(new Tuple2(item.get("userid").getS(), item.get("newsid").getS()));
				}
			
				// Create a new list of tuples with article
				List<Tuple2<String, String>> news = new ArrayList<Tuple2<String, String>>();
				HashSet<String> art = new HashSet<String>();
				scanRequest = new ScanRequest().withTableName("news");
			
				// Scan through the news table and convert all records into an RDD
				res = dbclient.scan(scanRequest);
				for (Map<String, AttributeValue> item : res.getItems()){
					news.add(new Tuple2(item.get("category").getS(), item.get("newsid").getS()));
					art.add(item.get("newsid").getS());
				}
				
				// Create a new list of all users
				List<String> users = new ArrayList<String>();
				scanRequest = new ScanRequest().withTableName("users");
			
				// Scan through the friends table and convert all records into an RDD
				res = dbclient.scan(scanRequest);
				for (Map<String, AttributeValue> item : res.getItems()){
					users.add(item.get("userid").getS());
				}
			
				String jar = "models/nets212-hw3-0.0.1-SNAPSHOT.jar";
			
				System.out.printf("Uploading %s to the Spark context...\n", jar);
				client.uploadJar(new File(jar)).get();
			
				// Performing the job with back links
				System.out.printf("Running adsorption right now\n");
				List<Tuple2<String,Tuple2<String,Double>>> result = client.submit(new RecJob(friends, interests, likes, news, users)).get();
				System.out.println("Computation successful");
			
				// Filter out all article nodes from returned result
				List<Tuple2<String,Tuple2<String,Double>>> artNodes = new ArrayList<Tuple2<String,Tuple2<String,Double>>>();
				for (Tuple2<String,Tuple2<String,Double>> t : result) {
					if (art.contains(t._1)) {
						artNodes.add(t);
					}
				}
			
				DynamoDB db = new DynamoDB( 
						AmazonDynamoDBClientBuilder.standard()
						.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
								"https://dynamodb.us-east-1.amazonaws.com", "us-east-1"))
						.withCredentials(new DefaultAWSCredentialsProviderChain())
						.build());
			
				//Create Items for each word and group 25 of them into one batch to put in the table
				Collection<Item> wordSet = new HashSet<Item>();
				int x = 0;
				TableWriteItems indexTableWriteItems = new TableWriteItems("newsweights");
				for (Tuple2<String,Tuple2<String,Double>> row : artNodes) {
					//Creating the batches of 25
					Item item = new Item()
							.withPrimaryKey("newsid", row._1, "userid", row._2._1)
							.withNumber("weights", row._2._2);
					wordSet.add(item);
					x += 1;
					if (x == 25) {
						try {
							//Batch write to the DynamoDB database
							indexTableWriteItems.withItemsToPut(wordSet);
							BatchWriteItemOutcome outcome = db.batchWriteItem(indexTableWriteItems);
							do {
								
								// Check for unprocessed keys which could happen if you exceed
								// provisioned throughput
								
								Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();

								if (outcome.getUnprocessedItems().size() != 0) {
									outcome = db.batchWriteItemUnprocessed(unprocessedItems);
								}

							} while (outcome.getUnprocessedItems().size() > 0);
						} catch (Exception e) {
							e.printStackTrace(System.err);
						}
						x = 0;
						wordSet = new HashSet<Item>();
					}
				}
				//Put in remaining Items that are not grouped
				if (x != 25 && x != 0) {
					try {
						indexTableWriteItems.withItemsToPut(wordSet);
						BatchWriteItemOutcome outcome = db.batchWriteItem(indexTableWriteItems);
						do {

							// Check for unprocessed keys which could happen if you exceed
							// provisioned throughput

							Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();

							if (outcome.getUnprocessedItems().size() != 0) {
								outcome = db.batchWriteItemUnprocessed(unprocessedItems);
							}

						} while (outcome.getUnprocessedItems().size() > 0);
					} catch (Exception e) {
						System.err.println("Failed to retrieve items: ");
						e.printStackTrace(System.err);
					}
				}
				System.out.println("News weights update successful");
			
			} finally {
				client.stop(true);
			}
			TimeUnit.MINUTES.sleep(60);
		}
	}

}