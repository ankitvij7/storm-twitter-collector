package org.apache.storm.starter.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

/**
 * Storm spout class randomly samples and propagates a subset of hash tags.
 */
public class TwitterHashTagSpout extends BaseRichSpout {

    // Constants
    private static final int RANDOM_HASHTAG_SUBSET_SIZE = 14;
    private static final String HASHTAGS_OUTPUT_FIELD = "hashtags";
    private static final int NEXT_TUPLE_SLEEP_TIME_MILLISEC = 30000;
    private final static String[] TWITTER_HASHTAGS = {"#halloween", "#National", "#Sunday", "#week", "#viking",
            "#India", "#happy", "#Barcelona", "#California", "#Elon", "#Madison", "#twitter", "#Facebook", "#Trump",
            "#Netflix", "#awesome", "#funny", "#iPhone", "#apple", "#orange", "#subway", "#health", "#fitness", "#nice",
            "#weather", "#food", "#cats", "#trump", "#saint", "#thunder", "#thrones"};

    private SpoutOutputCollector spoutOutputCollector;

    @Override
    public void open(Map configuration, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        ArrayList<String> randomHashTagSet = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < RANDOM_HASHTAG_SUBSET_SIZE; i++) {
            randomHashTagSet.add(TWITTER_HASHTAGS[random.nextInt(TWITTER_HASHTAGS.length)]);
        }
        spoutOutputCollector.emit(new Values(randomHashTagSet));
        Utils.sleep(NEXT_TUPLE_SLEEP_TIME_MILLISEC);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(HASHTAGS_OUTPUT_FIELD));
    }
}
