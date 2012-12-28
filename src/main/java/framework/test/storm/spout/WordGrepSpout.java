package framework.test.storm.spout;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import framework.test.storm.SpoutSkeleton;

public class WordGrepSpout extends SpoutSkeleton {
	public static final String allChar = 
			"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
	
	private Random rand = null;
	private String sentences[] = null;
	private int sentenceLen = 100;
	
	public void setSentenceLen(final int len) {
		sentenceLen = len;
	}
	
	public int getSentenceLen() {
		return sentenceLen;
	}
	
	public WordGrepSpout() {
		setSpoutName("WordGrepSpout");
	}

	@Override
	public void spoutOpen(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		generateRandomWords();
		rand = new Random();
	}

	@Override
	public void spoutNextTuple(SpoutOutputCollector collector) {
		String sentence = sentences[rand.nextInt(sentences.length)];
		collector.emit(new Values(sentence, System.currentTimeMillis()));
	}

	@Override
	public void spoutDeclareOputputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence", "timestamp"));
	}
	
	private void generateRandomWords() {		
	    Random random = new Random();
	    int sentenceNum = 512;
	    sentences = new String[sentenceNum];
	    
	    for (int i = 0; i < sentenceNum; i++) {
	    	StringBuilder sb = new StringBuilder(); 
	    	for (int j = 0; j < sentenceLen; j++) { 
	    		sb.append(allChar.charAt(random.nextInt(allChar.length()))); 
	    	}
	    	sentences[i] = sb.toString();
	    }
	}
}
