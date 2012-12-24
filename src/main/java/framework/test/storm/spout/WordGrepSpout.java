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
	
	Random rand = null;
	String words[] = null;
	
	public WordGrepSpout() {
		setSpoutName("WordGrepSpout");
		setSpoutConcurrency(3);
	}

	@Override
	public void spoutOpen(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		generateRandomWords();
		rand = new Random();
	}

	@Override
	public void spoutNextTuple(SpoutOutputCollector collector) {
		String word = words[rand.nextInt(words.length)];
		collector.emit(new Values(word));
	}

	@Override
	public void spoutDeclareOputputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}
	
	private void generateRandomWords() {
		StringBuffer sb = new StringBuffer(); 
	    Random random = new Random();
	    int wordLength = 64;
	    int wordsNum = 512;
	    words = new String[wordsNum];
	    
	    for (int i = 0; i < wordsNum; i++) {
	    	for (int j = 0; j < wordLength; j++) { 
	    		sb.append(allChar.charAt(random.nextInt(allChar.length()))); 
	    	}
	    	words[i] = sb.toString();
	    }
	}
}
