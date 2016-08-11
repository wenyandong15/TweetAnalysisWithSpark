package org.TwitterAnalysisWithSpark;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.conf.*;
import twitter4j.auth.*;
import twitter4j.Status;
import org.apache.spark.api.java.function.Function;

public class SparkJavaTest {
    public static void main(String[] args) {


        JavaStreamingContext jsc = new JavaStreamingContext(
                new SparkConf().setAppName("SparkJavaTest").setMaster("local[2]"),
                Durations.seconds(2)
        );
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

        JavaReceiverInputDStream<Status> twitterStream =
                TwitterUtils.createStream(jsc, auth);

        // JavaDStream<String> tweetTexts = twitterStream.filter(tweet -> tweet.getLang()=="en").map((status) -> status.getText());
        JavaDStream<Status> enTwitterStream = twitterStream.filter(tweet -> tweet.getLang().equalsIgnoreCase("en"));

        // JavaDStream<String> tweetTexts = enTwitterStream.map(status -> status.getHashtagEntities());
        // tweetTexts.foreachRDD(tweets -> {
         //   tweets.collect().stream().forEach(t -> System.out.println(t));});
              //                         tweetTexts.foreachRDD(rdd -> {
            // rdd.foreach(a -> System.out.println(a));});

        enTwitterStream.print();
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


