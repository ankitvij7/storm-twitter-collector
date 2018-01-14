package storm.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.starter.bolt.TwitterCommonWordsFilterPrinterBolt;
import org.apache.storm.starter.bolt.TwitterFriendsHashTagFilterPrinterBolt;
import org.apache.storm.starter.spout.TwitterFriendsCountSpout;
import org.apache.storm.starter.spout.TwitterHashTagSpout;
import org.apache.storm.starter.spout.TwitterSampleSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;


/**
 * Storm topology to print 50% of the most common words in time intervals of 30 seconds
 */
public class FindCommonWords {

    // Constants
    private final static String[] KEYWORDS = {"Halloween", "national", "Sunday", "week", "viking", "India", "happy",
            "Barcelona", "California", "Elon", "Madison", "twitter", "Facebook", "Trump", "Netflix", "awesome", "funny",
            "iPhone", "apple", "orange", "subway", "health", "fitness"};
    private final static String HDFS_URI = "hdfs://10.254.0.141:8020";
    private final static String PRINT_FILTERED_TWEETS_TO_HDFS_BOLD_ID = "printFilteredTweetsToHdfs";
    private final static String PRINT_COMMON_WORDS_TO_HDSH_BOLD_ID = "printCommonWordsToHdfs";
    private final static String TWITTER_INPUT_STREAM_SPOUT_ID = "twitterInputStream";
    private final static String TWITTER_HASHTAGS_SPOUT_ID = "twitterHashTags";
    private final static String TWITTER_FRIENDS_COUNT_SPOUT_ID = "twitterFriendsCount";
    private final static String COMMON_WORDS_IN_TWEETS_TOPOLOGY = "FindCommonWords";
    private final static String TWEETS_FILTER_BOLT_ID = "twitterFriendsHashTagFilterPrinter";
    private final static String TWEETS_COMMON_WORDS_BOLT_ID = "twitterCommonWordsFilterPrinter";
    private final static String FILTERED_TWEETS_FILE_PATH = "PartBQuestion2_filteredTweets_output";
    private final static String COMMON_WORDS_FILE_PATH = "PartBQuestion2_commonWords_output";
    private final static String TXT_FILE_EXTENSION = ".txt";
    private final static String LOCAL_MODE = "local";
    private final static int FILESYSTEM_SYNC_TUPLE_COUNT = 1000;
    private final static float FILE_ROTATION_COUNT = 5.0f;
    private final static int BOLT_TASK_PARALLELISM = 4;
    private final static int WORKER_COUNT = 15;
    private final static int MAX_SPOUT_PENDING = 4000;
    private final static int LOCAL_CLUSTER_SLEEP_TIME_MILLISEC = 1000000;
    private final static String HDFS_RECORD_FIELD_DELIMITER = "|";


    /**
     * Main method that builds and submits the topology
     *
     * @param args
     * @throws InvalidTopologyException
     * @throws AuthorizationException
     * @throws AlreadyAliveException
     */
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException,
            AlreadyAliveException {
        String consumerKey = "hld0SfLqOIJ6e687JBTNU7Vii";//args[0];
String consumerSecret = "T8n3rl4RZu6RyRWVphyK7KdhrpY2WGQ7c2PH4GAsPfFEre1LQw";//args[1];
String accessToken = "137976812-Wa6s6qZ08pABi4EpMSpfbXNrJ6TeKOlteXfwwjWz";//args[2];
String accessTokenSecret = "yzdH7Ieo29RXuHcyvVltbJO08sg17j8H7CA58IDzOKK37";//args[3];
        boolean isLocalMode = true;//args[4].equalsIgnoreCase(LOCAL_MODE);

        Config stormConfig = new Config();
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        String filteredTweetsFilePath = FILTERED_TWEETS_FILE_PATH;
        String commonWordsFilePath = COMMON_WORDS_FILE_PATH;

        if (isLocalMode) {
            filteredTweetsFilePath += TXT_FILE_EXTENSION;
            commonWordsFilePath += TXT_FILE_EXTENSION;
        }

        // Setup spouts
        TwitterSampleSpout twitterSampleSpout = new TwitterSampleSpout(consumerKey, consumerSecret, accessToken,
                accessTokenSecret, KEYWORDS);
        TwitterFriendsCountSpout twitterFriendsCountSpout = new TwitterFriendsCountSpout();
        TwitterHashTagSpout twitterHashTagSpout = new TwitterHashTagSpout();

        topologyBuilder.setSpout(TWITTER_INPUT_STREAM_SPOUT_ID, twitterSampleSpout);
        topologyBuilder.setSpout(TWITTER_FRIENDS_COUNT_SPOUT_ID, twitterFriendsCountSpout);
        topologyBuilder.setSpout(TWITTER_HASHTAGS_SPOUT_ID, twitterHashTagSpout);


        // Setup bolts
        TwitterFriendsHashTagFilterPrinterBolt twitterFriendsHashTagFilterPrinterBolt =
                new TwitterFriendsHashTagFilterPrinterBolt(filteredTweetsFilePath, isLocalMode);
        topologyBuilder.setBolt(TWEETS_FILTER_BOLT_ID, twitterFriendsHashTagFilterPrinterBolt)
                .shuffleGrouping(TWITTER_FRIENDS_COUNT_SPOUT_ID).shuffleGrouping(TWITTER_INPUT_STREAM_SPOUT_ID)
                .shuffleGrouping(TWITTER_HASHTAGS_SPOUT_ID);

        TwitterCommonWordsFilterPrinterBolt twitterCommonWordsFilterPrinterBolt =
                new TwitterCommonWordsFilterPrinterBolt(commonWordsFilePath, isLocalMode);
        topologyBuilder.setBolt(TWEETS_COMMON_WORDS_BOLT_ID, twitterCommonWordsFilterPrinterBolt).globalGrouping
                (TWEETS_FILTER_BOLT_ID);

        if (isLocalMode) {
            // Local cluster for outputs to local file systems
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology(COMMON_WORDS_IN_TWEETS_TOPOLOGY, stormConfig, topologyBuilder.createTopology());
            Utils.sleep(LOCAL_CLUSTER_SLEEP_TIME_MILLISEC);
            localCluster.shutdown();
        } else {
            RecordFormat recordFormat = new DelimitedRecordFormat().withFieldDelimiter(HDFS_RECORD_FIELD_DELIMITER);
            SyncPolicy syncPolicy = new CountSyncPolicy(FILESYSTEM_SYNC_TUPLE_COUNT);
            FileRotationPolicy rotationPolicy =
                    new FileSizeRotationPolicy(FILE_ROTATION_COUNT, FileSizeRotationPolicy.Units.MB);

            HdfsBolt printFilteredTweetsHdfsBolt = new HdfsBolt()
                    .withFsUrl(HDFS_URI)
                    .withFileNameFormat(new DefaultFileNameFormat().withPath(filteredTweetsFilePath))
                    .withRecordFormat(recordFormat)
                    .withRotationPolicy(rotationPolicy)
                    .withSyncPolicy(syncPolicy);

            HdfsBolt printCommonWordsHdfsBolt = new HdfsBolt()
                    .withFsUrl(HDFS_URI)
                    .withFileNameFormat(new DefaultFileNameFormat().withPath(commonWordsFilePath))
                    .withRecordFormat(recordFormat)
                    .withRotationPolicy(rotationPolicy)
                    .withSyncPolicy(syncPolicy);

            topologyBuilder.setBolt(PRINT_FILTERED_TWEETS_TO_HDFS_BOLD_ID, printFilteredTweetsHdfsBolt,
                    BOLT_TASK_PARALLELISM).shuffleGrouping(TWEETS_FILTER_BOLT_ID);

            topologyBuilder.setBolt(PRINT_COMMON_WORDS_TO_HDSH_BOLD_ID, printCommonWordsHdfsBolt,
                    BOLT_TASK_PARALLELISM).shuffleGrouping(TWEETS_COMMON_WORDS_BOLT_ID);

            stormConfig.setNumWorkers(WORKER_COUNT);
            stormConfig.setMaxSpoutPending(MAX_SPOUT_PENDING);

            StormSubmitter.submitTopology(COMMON_WORDS_IN_TWEETS_TOPOLOGY, stormConfig, topologyBuilder
                    .createTopology());
        }
    }
}
