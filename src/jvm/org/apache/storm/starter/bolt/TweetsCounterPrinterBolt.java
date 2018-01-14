package org.apache.storm.starter.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import twitter4j.Status;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * Storm bolt class that counts the tweets
 */
public class TweetsCounterPrinterBolt extends BaseRichBolt {

    // Constants
    private final static String COLLECT_TWEETS_TOPOLOGY = "CollectTweets";
    private final static String TWEET_OUTPUT_FIELD = "tweet";
    private final static int MAX_TWEET_COLLECTION_LIMIT = 3000000;

    private OutputCollector outputCollector;
    private int currentTweetsCount = 0;
    private String outputFilePath;
    private boolean printToLocal;

    /**
     * Constructor
     *
     * @param outputFilePath
     * @param printToLocal
     */
    public TweetsCounterPrinterBolt(String outputFilePath, boolean printToLocal) {
        this.outputFilePath = outputFilePath;
        this.printToLocal = printToLocal;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
    }

    @Override
    public void execute(Tuple tuple) {

        // Kill topology when the maximum number of tweets have been collected
        if (currentTweetsCount == MAX_TWEET_COLLECTION_LIMIT) {
            try {
                NimbusClient.getConfiguredClient(Utils.readStormConfig()).getClient()
                        .killTopology(COLLECT_TWEETS_TOPOLOGY);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        currentTweetsCount++;

        String outPutTweet = ((Status) tuple.getValue(0)).getText();

        if (printToLocal) {
            try {
                FileWriter fileWriter = new FileWriter(outputFilePath, true);
                fileWriter.write(outPutTweet+ "\n\n");
                fileWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        outputCollector.emit(new Values(outPutTweet));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(TWEET_OUTPUT_FIELD));
    }

}
