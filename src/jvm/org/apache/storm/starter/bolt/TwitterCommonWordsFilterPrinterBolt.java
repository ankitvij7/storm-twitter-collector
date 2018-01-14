package org.apache.storm.starter.bolt;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Collections;
import java.util.Comparator;
import java.util.Arrays;
import java.util.List;

/**
 * Storm bolt that finds the 50% of common words in a timestamp
 */
public class TwitterCommonWordsFilterPrinterBolt extends BaseRichBolt {

    // Constants
    private final static String COMMON_WORD_OUTPUT_FIELD = "commonWords";
    private final static String[] STOP_WORDS = {"a", "about", "above", "after", "again", "against", "all", "am", "an",
            "and", "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being", "below", "between",
            "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't",
            "doing", "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't", "has",
            "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers",
            "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into",
            "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself",
            "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves",
            "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so",
            "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there",
            "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to",
            "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were",
            "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's",
            "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've",
            "your", "yours", "yourself", "yourselves"};
    private ArrayList<String> stopWords = new ArrayList<>(Arrays.asList(STOP_WORDS));
    private String outputFilePath;
    private boolean printToLocal;
    private OutputCollector outputCollector;

    /**
     * Constructor
     *
     * @param outputFilePath
     * @param printToLocal
     */
    public TwitterCommonWordsFilterPrinterBolt(String outputFilePath,
                                               boolean printToLocal) {
        this.outputFilePath = outputFilePath;
        this.printToLocal = printToLocal;
    }

    @Override
    public void prepare(Map configuration, TopologyContext context, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        HashMap<String, Integer> wordCounts = new HashMap<>();
        List<String> tweetWords = Arrays.asList(((String) tuple.getValue(0)).split("\\s+"));
        for (String tweetWord : tweetWords) {
            // Remove re-tweet keyword  and hash tag from tweet word
            tweetWord = tweetWord.replaceAll("#", "").replaceAll("rt", "").replaceAll("RT", "");

            // Add tweet words that are not stop words
            if (!tweetWord.equals("") && tweetWord.matches("[a-zA-Z0-9]+") && !stopWords.contains(tweetWord)) {
                if (wordCounts.containsKey(tweetWord)) {
                    wordCounts.put(tweetWord, wordCounts.get(tweetWord) + 1);
                } else {
                    wordCounts.put(tweetWord, 1);
                }
            }
        }

        // Sort the word counts map
        List<Map.Entry<String, Integer>> sortedWordCountList = new ArrayList<>(wordCounts.entrySet());
        Collections.sort(sortedWordCountList, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> entry1, Map.Entry<String, Integer> entry2) {
                return (entry2.getValue()).compareTo(entry1.getValue());
            }
        });

        // Find 50% common words i.e the first half of the sorted list
        StringBuilder commonWords = new StringBuilder();
        for (int i = 0; i < sortedWordCountList.size()/2; i++) {
            commonWords.append(" ");
            commonWords.append(sortedWordCountList.get(i).getKey());
        }
        commonWords.append("\n\n");

        if (printToLocal) {
            try {
                FileWriter fileWriter = new FileWriter(outputFilePath, true);
                fileWriter.write(commonWords.toString());
                fileWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        outputCollector.emit(new Values(commonWords.toString()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(COMMON_WORD_OUTPUT_FIELD));
    }
}
