package org.TwitterAnalysisWithSpark;

import java.io.Serializable;
import com.datastax.driver.mapping.annotations.*;

/**
 * Created with IntelliJ IDEA.
 * User: wenyan
 * Date: 8/26/16
 * Time: 9:13 PM
 * To change this template use File | Settings | File Templates.
 */

public class TweetText implements Serializable {
    private long id;

    private String tweet;

    public TweetText() { }

    public TweetText(long id, String tweet) {
        this.id = id;
        this.tweet = tweet;
    }

    public long getId() {return id;}
    public void setId(long id) {this.id = id;}

    public String getTweet()  {return tweet;}
    public void setTweet(String tweet) {this.tweet = tweet;}

}
