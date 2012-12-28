package framework.test.storm.bolt;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import framework.test.storm.BoltSkeleton;

public class WordGrepBolt extends BoltSkeleton {
	static Logger log = Logger.getLogger(WordGrepBolt.class);
	
	String grepWord = null;
	
	public WordGrepBolt() {
		setBoltName("WordGrepBolt");
	}
	
	public void setGrepWord(String word) {
		grepWord = word;
	}
	
	public String getGrepWord() {
		return grepWord;
	}
	
	@Override
	public void boltPrepare(Map stormConf, TopologyContext context) {
		
	}

	@Override
	public void boltExecute(Tuple input, BasicOutputCollector collector) {
		String sentence = input.getStringByField("sentence");	
		if (sentence.indexOf(grepWord) != -1) {
			log.info("found sentence " + sentence);	
		}
		
		//sample tuple latency in every 100 tuples
		if (getRecvCounter() % 1000 == 0) {
			long ts = input.getLongByField("timestamp");
			long currTs = System.currentTimeMillis();
			log.info("tuple latency in ms[" + (currTs - ts) + "]");
		}
		
		//collector.emit(new Values(word, System.currentTimeMillis()));	
	}

	@Override
	public void boltDeclareOutputFields(OutputFieldsDeclarer declarer) {
		//declarer.declare(new Fields("sentence", "timestamp"));		
	}
	
	@Override
	public void setGrouping(String groupingSource,
			InputDeclarer<? extends InputDeclarer> declarer) {
		declarer.shuffleGrouping(groupingSource);
		
	}
}
