package framework.test.storm.bolt;

import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import framework.test.storm.BoltSkeleton;

public class TimestampBolt extends BoltSkeleton {

	@Override
	public void boltPrepare(Map stormConf, TopologyContext context) {
		setBoltName("TimestampBolt");
	}

	@Override
	public void boltExecute(Tuple input, BasicOutputCollector collector) {
		List<Object> values = input.getValues();
		
		
	}
	
	@Override
	public void boltDeclareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
}
