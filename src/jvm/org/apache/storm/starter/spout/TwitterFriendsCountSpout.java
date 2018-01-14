package org.apache.storm.starter.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Storm spout class that randomly samples and pick a number from the list of friends counts
 */
public class TwitterFriendsCountSpout extends BaseRichSpout {

    // Constants
    private static final String FRIENDS_COUNT_OUTPUT_FIELD = "friendsCount";
    private static final int NEXT_TUPLE_SLEEP_TIME_MILLISEC = 30000;
    private final static int[] TWITTER_FRIENDS_COUNT = {1000, 500, 3000, 900, 2000, 4000, 6000, 9000, 700, 5000, 8000};

    private SpoutOutputCollector spoutOutputCollector;

    @Override
    public void open(Map configuration, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        spoutOutputCollector.emit(new Values(TWITTER_FRIENDS_COUNT[new Random().nextInt(TWITTER_FRIENDS_COUNT.length)]));
        Utils.sleep(NEXT_TUPLE_SLEEP_TIME_MILLISEC);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FRIENDS_COUNT_OUTPUT_FIELD));
    }

}
