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
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.starter.bolt.TweetsCounterPrinterBolt;
import org.apache.storm.starter.spout.TwitterSampleSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Collect tweets which match the keywords and store them either in your local file system or HDFS.
 */
public class CollectTweets {

    // Constants
    private final static String[] KEYWORDS = {"Halloween", "national", "Sunday", "week", "viking", "India", "happy",
            "Barcelona", "California", "Elon", "Madison", "twitter", "Facebook", "Modi", "Netflix", "awesome", "fun",
            "iPhone", "apple", "orange", "subway", "health", "fitness"};
    private final static String HDFS_URL = "hdfs://10.254.0.141:8020";
    private final static String PRINT_TO_HDFS_BOLD_ID = "printToHdfs";
    private final static String TWEET_COUNTER_BOLD_ID = "tweetsCounterPrinter";
    private final static String TWITTER_INPUT_STREAM_SPOUT_ID = "twitterInputStream";
    private final static String COLLECT_TWEETS_TOPOLOGY = "CollectTweets";
    private final static String TXT_FILE_EXTENSION = ".txt";
    private final static int FILESYSTEM_SYNC_TUPLE_COUNT = 1000;
    private final static float FILE_ROTATION_COUNT = 5.0f;
    private final static int BOLT_TASK_PARALLELISM = 4;
    private final static int WORKER_COUNT = 20;
    private final static int MAX_SPOUT_PENDING = 5000;
    private final static int LOCAL_CLUSTER_SLEEP_TIME_MILLISEC = 1000000;
    private final static String LOCAL_MODE = "local";
    private final static String HDFS_RECORD_FIELD_DELIMITER = "|";
    private final static String OUTPUT_FILE_PATH = "PartBQuestion1_output";

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
        String consumerKey = args[0];
        String consumerSecret = args[1];
        String accessToken = args[2];
        String accessTokenSecret = args[3];
        boolean isLocalMode = args[4].equalsIgnoreCase(LOCAL_MODE);

        String tweetsFilePath = OUTPUT_FILE_PATH;
        
        if (isLocalMode) {
            tweetsFilePath += TXT_FILE_EXTENSION;
        }

        Config stormConfig = new Config();
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        TwitterSampleSpout twitterSampleSpout = new TwitterSampleSpout(consumerKey, consumerSecret, accessToken,
                accessTokenSecret, KEYWORDS);
        topologyBuilder.setSpout(TWITTER_INPUT_STREAM_SPOUT_ID, twitterSampleSpout);
        topologyBuilder.setBolt(TWEET_COUNTER_BOLD_ID, new TweetsCounterPrinterBolt(tweetsFilePath, isLocalMode))
                .globalGrouping(TWITTER_INPUT_STREAM_SPOUT_ID);


        if (isLocalMode) {
            // Local cluster for outputs to local file systems
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology(COLLECT_TWEETS_TOPOLOGY, stormConfig, topologyBuilder.createTopology());
            Utils.sleep(LOCAL_CLUSTER_SLEEP_TIME_MILLISEC);
            localCluster.shutdown();
        } else {
            // Hdfs bolt used for printing the tweets on hdfs
            HdfsBolt printHdfsBolt = new HdfsBolt()
                    .withFsUrl(HDFS_URL)
                    .withFileNameFormat(new DefaultFileNameFormat().withPath(tweetsFilePath))
                    .withRecordFormat(new DelimitedRecordFormat().withFieldDelimiter(HDFS_RECORD_FIELD_DELIMITER))
                    .withRotationPolicy(new FileSizeRotationPolicy(FILE_ROTATION_COUNT, FileSizeRotationPolicy.Units.MB))
                    .withSyncPolicy(new CountSyncPolicy(FILESYSTEM_SYNC_TUPLE_COUNT));

            topologyBuilder.setBolt(PRINT_TO_HDFS_BOLD_ID, printHdfsBolt, BOLT_TASK_PARALLELISM)
                    .shuffleGrouping(TWEET_COUNTER_BOLD_ID);

            stormConfig.setNumWorkers(WORKER_COUNT);
            stormConfig.setMaxSpoutPending(MAX_SPOUT_PENDING);

            StormSubmitter.submitTopology(COLLECT_TWEETS_TOPOLOGY, stormConfig, topologyBuilder.createTopology());
        }
    }
}
