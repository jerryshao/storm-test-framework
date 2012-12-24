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
		setBoltConcurrency(3);
	}
	
	public void setGrepWord(String word) {
		grepWord = word;
	}
	
	@Override
	public void boltPrepare(Map stormConf, TopologyContext context) {
		
	}

	@Override
	public void boltExecute(Tuple input, BasicOutputCollector collector) {
		String word = input.getStringByField("sentence");
		if (word.indexOf(grepWord) != -1) {
			log.info("found sentence " + word);
			collector.emit(new Values(word));
		}	
	}

	@Override
	public void boltDeclareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));		
	}
	
	@Override
	public void setGrouping(String groupingSource,
			InputDeclarer<? extends InputDeclarer> declarer) {
		declarer.shuffleGrouping(groupingSource);
		
	}
}
