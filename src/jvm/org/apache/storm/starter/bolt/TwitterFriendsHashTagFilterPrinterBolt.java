package org.apache.storm.starter.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.Status;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Storm bolt that filters tweets based on friends  end counts and hash tags
 */
public class TwitterFriendsHashTagFilterPrinterBolt extends BaseRichBolt {

    // Constants
    private final static String TWEET_OUTPUT_FIELD = "filteredTweet";
    private final static String TWITTER_INPUT_STREAM_SPOUT_ID = "twitterInputStream";
    private final static String TWITTER_HASHTAGS_SPOUT_ID = "twitterHashTags";
    private final static String TWITTER_FRIENDS_COUNT_SPOUT_ID = "twitterFriendsCount";

    private int friendsCount = 0;
    private ArrayList<String> hashTags = new ArrayList<>();
    private OutputCollector outputCollector;
    private Queue<Status> tweets = new LinkedList<>();
    private long timestamp = 0L;
    private long previousTimestamp = 0L;
    private String outputFilePath;
    private boolean printToLocal;
    private StringBuilder outputTweets = new StringBuilder();
    private int countOfFilteringStreams = 0;

    /**
     * Constructor
     *
     * @param outputFilePath
     * @param printToLocal
     */
    public TwitterFriendsHashTagFilterPrinterBolt(String outputFilePath, boolean printToLocal) {
        this.outputFilePath = outputFilePath;
        this.printToLocal = printToLocal;
    }

    @Override
    public void execute(Tuple tuple) {

        // Track twitter streams
        if (tuple.getSourceComponent().equals(TWITTER_INPUT_STREAM_SPOUT_ID)) {
                tweets.add((Status) tuple.getValue(0));
        }

        // Track friends count stream
        if (tuple.getSourceComponent().equals(TWITTER_FRIENDS_COUNT_SPOUT_ID)) {
            friendsCount = (int) tuple.getValue(0);
            countOfFilteringStreams++;
        }

        // Track hash tags stream
        if (tuple.getSourceComponent().equals(TWITTER_HASHTAGS_SPOUT_ID)) {
            hashTags = (ArrayList<String>) tuple.getValue(0);
            countOfFilteringStreams++;
        }

        if (countOfFilteringStreams == 2) {
            timestamp = new Date().getTime();
            countOfFilteringStreams = 0;
        }

        while (!tweets.isEmpty() && !hashTags.isEmpty() && friendsCount != 0) {
                filterTweets(tweets.poll(), timestamp);
        }

    }

    @Override
    public void prepare(Map stormConfig, TopologyContext context, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(TWEET_OUTPUT_FIELD));
    }

    /**
     * Filter tweets based on friends count and hash tags
     *
     * @param tweetStatus
     * @param timestamp
     */
    private void filterTweets(Status tweetStatus, Long timestamp) {

        boolean passedFriendsCountsFilter = tweetStatus != null && tweetStatus.getUser() != null &&
                tweetStatus.getUser().getFriendsCount() < friendsCount;

        boolean passedHashTagsFilter = false;
        for (String hashTag : hashTags) {
            if (tweetStatus.getText().toLowerCase().contains(hashTag.toLowerCase())) {
                passedHashTagsFilter = true;
                break;
            }
        }

        // Collect all the tweets in an interval and output as one blob
        if (passedFriendsCountsFilter && passedHashTagsFilter) {
            String tweet = tweetStatus.getText();
            if (previousTimestamp != timestamp && previousTimestamp != 0L) {
                outputTweets.append("\n\n");
                String tweetCollection = outputTweets.toString();
                outputTweets = new StringBuilder();
                outputTweets.append(tweet.replaceAll("\n", " "));
                outputTweets.append("\n");
                previousTimestamp = timestamp;

                if (printToLocal) {
                    try {
                        FileWriter fileWriter = new FileWriter(outputFilePath, true);
                        fileWriter.write(tweetCollection);
                        fileWriter.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                outputCollector.emit(new Values(tweetCollection));
            } else {
                outputTweets.append(tweet.replaceAll("\n", " "));
                outputTweets.append("\n");
                previousTimestamp = timestamp;
            }
        }
    }
}
