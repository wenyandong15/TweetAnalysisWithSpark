package org.TwitterAnalysisWithSpark;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraStreamingJavaUtil;
import org.apache.spark.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.TwitterUtils;
// import org.apache.spark.serializer.KryoSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import twitter4j.conf.*;
import twitter4j.auth.*;
import twitter4j.Status;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import org.apache.spark.api.java.function.VoidFunction;



import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

public class SparkJavaTest {
    public static void main(String[] args) {

        // String[] jarPaths = new String[1];

        // jarPaths[0] =    "/Users/wenyan/git/TweetAnalysisWithSpark/target/TweetAnalysisWithSpark-1.0-SNAPSHOT.jar";

        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkJavaTest")
                .setMaster("local[2]").set("spark.cassandra.connection.host", "127.0.0.1")
                // .set("spark.serializer", KryoSerializer.class.getName())
                .set("spark.cassandra.connection.host", "localhost")
                .set("es.nodes", "localhost:9200")
                .set("es.index.auto.create", "true");


        JavaStreamingContext jsc = new JavaStreamingContext(
                sparkConf, Durations.seconds(2)
        );


        // Build Twitter OAuth configuration
        /*
            Configuration conf = new  ConfigurationBuilder()
                .setOAuthConsumerKey("Z9nwckWtSbxKzFssN9L6y5nyb")
                .setOAuthConsumerSecret("5ytZbE8iTqkLH69Vf33DrvDKQil0SMQm6NBI1YtVUyEtpWZmdG") //shhhhh don't tell
                .setOAuthAccessToken("1672351662-aX2N8ZZvVit1Jz4xn5JkL61FrnErcVlNpJiBVXC")
                .setOAuthAccessTokenSecret("sA0ofmTH7DaYm5BmPrRy6zYo9Nq8iBEj8yv9Y9kE33m0O")
                .build();
            Authorization auth = new OAuthAuthorization(conf);
        */
        Configuration twitterConf = ConfigurationContext.getInstance();
        Authorization auth = AuthorizationFactory.getInstance(twitterConf);

        ObjectMapper mapper = new ObjectMapper();

        JavaReceiverInputDStream<Status> twitterStream =
                TwitterUtils.createStream(jsc, auth);


        JavaDStream<Status> enTwitterStream = twitterStream.filter(tweet -> tweet.getLang().equalsIgnoreCase("en")).cache();

        JavaDStream<TweetText> enTwitterTextStream = enTwitterStream.map( status -> new TweetText(status.getId(), status.getText()));
        enTwitterTextStream.map(t -> mapper.writeValueAsString(t)).foreachRDD(new VoidFunction<JavaRDD<String>>() {
                    @Override
                    public void call(JavaRDD<String> rdd) {
                        JavaEsSpark.saveJsonToEs(rdd, "spark/tweets");
                    }
                });


        // JavaDStream<String> tweetTexts = enTwitterStream.map(status -> status.getHashtagEntities());
        // tweetTexts.foreachRDD(tweets -> {
         //   tweets.collect().stream().forEach(t -> System.out.println(t));});
              //                         tweetTexts.foreachRDD(rdd -> {
            // rdd.foreach(a -> System.out.println(a));});

        // enTwitterStream.print();
        CassandraStreamingJavaUtil.javaFunctions(enTwitterTextStream).writerBuilder("dev", "tweet_text", CassandraJavaUtil.mapToRow(TweetText.class)).saveToCassandra();



        jsc.start();

        // statuses.saveAsHadoopFiles("hdfs://HadoopSystem-150s:8020/Spark_Twitter_out","txt");
        jsc.awaitTermination();

        // Each tweet comes as a twitter4j.Status object, which we can use to
        // extract hash tags. We use flatMap() since each status could have
        // ZERO OR MORE hashtags.
        // val hashTags = stream.flatMap(status => status.getHashtagEntities)

        // Convert hashtag to (hashtag, 1) pair for future reduction.
        // val hashTagPairs = hashTags.map(hashtag => ("#" + hashtag.getText, 1))

        // Use reduceByKeyAndWindow to reduce our hashtag pairs by summing their
        // counts over the last 10 seconds of batch intervals (in this case, 2 RDDs).
        // val topCounts10 = hashTagPairs.reduceByKeyAndWindow((l, r) => {l + r}, Seconds(10))

        // topCounts10 will provide a new RDD for every window. Calling transform()
        // on each of these RDDs gives us a per-window transformation. We use
        // this transformation to sort each RDD by the hashtag counts. The FALSE
        // flag tells the sortBy() function to sort in descending order.
        // val sortedTopCounts10 = topCounts10.transform(rdd =>
          //      rdd.sortBy(hashtagPair => hashtagPair._2, false))

        // Print popular hashtags.
        // sortedTopCounts10.foreachRDD(rdd => {
         //       val topList = rdd.take(10)
         //       println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
          //      topList.foreach{case (tag, count) => println("%s (%d tweets)".format(tag, count))}
        //})




    }

}


