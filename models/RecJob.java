package edu.upenn.cis.nets212.hw3.livy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.livy.Job;
import org.apache.livy.JobContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import edu.upenn.cis.nets212.storage.SparkConnector;
import scala.Tuple2;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

public class RecJob implements Job<List<Tuple2<String, Tuple2<String, Double>>>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Connection to Apache Spark
	 */
	SparkSession spark;
	
	JavaSparkContext context;
	
	List<Tuple2<String, String>> friend;
	List<Tuple2<String, String>> interest;
	List<Tuple2<String, String>> like;
	List<Tuple2<String, String>> article;
	List<String> users;
	

	/**
	 * Initialize the database connection and open the file
	 * 
	 * @throws IOException
	 * @throws InterruptedException 
	 * @throws DynamoDbException 
	 */
	public void initialize() throws IOException, InterruptedException {
		System.out.println("Connecting to Spark...");
		spark = SparkConnector.getSparkConnection();
		context = SparkConnector.getSparkContext();
		System.out.println("Connected!");
	}
	
	
	/**
	 * Main functionality in the program: run adsorption
	 * 
	 * @throws DynamoDbException DynamoDB is unhappy with something
	 * @throws InterruptedException User presses Ctrl-C
	 */
	public List<Tuple2<String, Tuple2<String, Double>>> run() throws IOException, InterruptedException {
		System.out.println("Running");
		
		double d = 0.00000001;
		int imax = 15;
		
		// Load all the RDDs
		JavaPairRDD<String, String> friends = context.parallelizePairs(friend);
		JavaPairRDD<String, String> interests = context.parallelizePairs(interest);
		JavaPairRDD<String, String> likes = context.parallelizePairs(like);
		JavaPairRDD<String, String> topics = context.parallelizePairs(article);
		JavaRDD<String> userids = context.parallelize(users);
		
		// Compute weights from users
		// Compute out edges from user to other user
		JavaPairRDD<String, Integer> outCounts = friends.aggregateByKey(0,
				(val, row) -> val + 1,
				(val, val2) -> val + val2);
		
		JavaPairRDD<String, Tuple2<String, Double>> users = friends.join(outCounts)
													.mapToPair(row -> new Tuple2<String, Tuple2<String, Double>>(row._1, 
															new Tuple2<String, Double>(row._2._1, 1.0 / row._2._2 * 0.3)));
		
		// Compute out edges from user to topics
		outCounts = interests.aggregateByKey(0,
				(val, row) -> val + 1,
				(val, val2) -> val + val2);
		
		JavaPairRDD<String, Tuple2<String, Double>> categories = interests.join(outCounts)
													.mapToPair(row -> new Tuple2<String, Tuple2<String, Double>>(row._1, 
															new Tuple2<String, Double>(row._2._1, 1.0 / row._2._2 * 0.3)));
		
		// Compute out edges from user to news articles they liked
		outCounts = likes.aggregateByKey(0,
				(val, row) -> val + 1,
				(val, val2) -> val + val2);
				
		JavaPairRDD<String, Tuple2<String, Double>> news = likes.join(outCounts)
													.mapToPair(row -> new Tuple2<String, Tuple2<String, Double>>(row._1, 
															new Tuple2<String, Double>(row._2._1, 1.0 / row._2._2 * 0.4)));
		
		// Compute weights from categories
		JavaPairRDD<String, String> reverseInterests = interests.mapToPair(row -> new Tuple2<String, String>(row._2, row._1));
		JavaPairRDD<String, String> reverseTopics = topics.mapToPair(row -> new Tuple2<String, String>(row._2, row._1));
		JavaPairRDD<String, String> allCategories = reverseInterests.union(reverseTopics);
		
		outCounts = allCategories.aggregateByKey(0,
				(val, row) -> val + 1,
				(val, val2) -> val + val2);
		
		JavaPairRDD<String, Tuple2<String, Double>> cats = allCategories.join(outCounts)
													.mapToPair(row -> new Tuple2<String, Tuple2<String, Double>>(row._1, 
															new Tuple2<String, Double>(row._2._1, 1.0 / row._2._2)));
		
		// Compute weights from articles
		JavaPairRDD<String, String> reverseLikes = likes.mapToPair(row -> new Tuple2<String, String>(row._2, row._1));
		JavaPairRDD<String, String> allArticles = reverseLikes.union(topics);
		
		outCounts = allArticles.aggregateByKey(0,
				(val, row) -> val + 1,
				(val, val2) -> val + val2);
		
		JavaPairRDD<String, Tuple2<String, Double>> articles = allArticles.join(outCounts)
													.mapToPair(row -> new Tuple2<String, Tuple2<String, Double>>(row._1, 
															new Tuple2<String, Double>(row._2._1, 1.0 / row._2._2)));
		
		// Compute union of all edges
		JavaPairRDD<String, Tuple2<String, Double>> allEdges = news.union(cats).union(articles).union(users).union(categories).distinct();
		
		// Initiate all user nodes with weight 1.0 
		JavaPairRDD<String, Tuple2<String, Double>> weights = userids.distinct().mapToPair(row -> new Tuple2<String, 
															Tuple2<String, Double>>(row, new Tuple2<String, Double>(row, 1.0)));
		
		JavaPairRDD<Tuple2<String, String>, Double> adsorped;
		JavaPairRDD<Tuple2<String, String>, Double> adsorpedLast;
		JavaPairRDD<String, Double> unormalized;
		
		// Distribute the weights
		adsorped = weights.join(allEdges).mapToPair(row -> new Tuple2<Tuple2<String, String>, Double>(new
																	Tuple2<String, String>(row._2._2._1, row._2._1._1), row._2._1._2 * row._2._2._2));
					
		adsorped = adsorped.aggregateByKey(0.0,
										(val, row) -> val + row,
										(val, val2) -> val + val2);
					
		// Normalize the weights
		unormalized = adsorped.mapToPair(row -> new Tuple2<String, Double>(row._1._1, row._2));
		unormalized = unormalized.aggregateByKey(0.0,
											(val, row) -> val + row,
											(val, val2) -> val + val2);
		weights = adsorped.mapToPair(row -> new Tuple2<String, Tuple2<String, Double>>(row._1._1, new Tuple2<String, Double>(row._1._2, row._2)));
		weights = weights.join(unormalized).mapToPair(row -> new Tuple2<String, Tuple2<String, Double>>(row._1, 
																			new Tuple2<String, Double>(row._2._1._1, row._2._1._2 / row._2._2)));
		
		adsorpedLast = weights.mapToPair(row -> new Tuple2<Tuple2<String, String>, Double>(new Tuple2(row._1, row._2._1), row._2._2));
		
		// Iteratively conduct adsorption up to 15 rounds
		for (int i = 0; i < imax - 1; i++) {
			// Distribute the weights
			adsorped = weights.join(allEdges).mapToPair(row -> new Tuple2<Tuple2<String, String>, Double>(new
															Tuple2<String, String>(row._2._2._1, row._2._1._1), row._2._1._2 * row._2._2._2));
			
			adsorped = adsorped.aggregateByKey(0.0,
								(val, row) -> val + row,
								(val, val2) -> val + val2);
			
			// Normalize the weights
			unormalized = adsorped.mapToPair(row -> new Tuple2<String, Double>(row._1._1, row._2));
			unormalized = unormalized.aggregateByKey(0.0,
									(val, row) -> val + row,
									(val, val2) -> val + val2);
			weights = adsorped.mapToPair(row -> new Tuple2<String, Tuple2<String, Double>>(row._1._1, new Tuple2<String, Double>(row._1._2, row._2)));
			weights = weights.join(unormalized).mapToPair(row -> new Tuple2<String, Tuple2<String, Double>>(row._1, 
																	new Tuple2<String, Double>(row._2._1._1, row._2._1._2 / row._2._2)));
			
			// Compare weight changes
			JavaPairRDD<Tuple2<String, String>, Double> current = weights.mapToPair(row -> new Tuple2<Tuple2<String, String>, Double>(new Tuple2(row._1, row._2._1), row._2._2));
			JavaPairRDD<Tuple2<String, String>, Double> changes = current.fullOuterJoin(adsorpedLast).mapToPair(row -> {
				Double diff;
				if (!row._2._1.isPresent()) {
					diff = row._2._2.get();
				} else if (!row._2._2.isPresent()) {
					diff = row._2._1.get();
				} else {
					diff = Math.abs(row._2._1.get() - row._2._2.get());
				}
				Tuple2<Tuple2<String, String>, Double> result = new Tuple2<Tuple2<String, String>, Double>(row._1, diff);
				return result;
			});
			
			Double epsilon = changes.map(row -> row._2).aggregate(0.0, 
					(val, row) -> Math.max(Math.abs(val), Math.abs(row)),
					(val1, val2) -> Math.max(Math.abs(val1), Math.abs(val2)));
			
			if (epsilon <= d) {
				break;
			}

			// Store weights after the current round
			adsorpedLast = current;
		}
		
		System.out.println("*** Finished adsorption! ***");
		ArrayList<Tuple2<String,Tuple2<String,Double>>> out = new ArrayList<Tuple2<String,Tuple2<String,Double>>>();
		List<Tuple2<String,Tuple2<String,Double>>> finaList = weights.collect();
		
		for (Tuple2<String,Tuple2<String,Double>> ele : finaList) {
			out.add(ele);
		}

		return out;
	}
	
	public RecJob(List<Tuple2<String, String>> friends, List<Tuple2<String, String>> interests, List<Tuple2<String, String>> likes, List<Tuple2<String, String>> news, List<String> user) {
		friend = friends;
		interest = interests;
		like = likes;
		article = news;
		users = user;
		System.setProperty("file.encoding", "UTF-8");
	}

	@Override
	public List<Tuple2<String, Tuple2<String, Double>>> call(JobContext arg0) throws Exception {
		initialize();
		return run();
	}

}