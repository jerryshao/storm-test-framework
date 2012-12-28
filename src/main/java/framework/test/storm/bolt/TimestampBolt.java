package framework.test.storm.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import framework.test.storm.BoltSkeleton;

public class TimestampBolt extends BoltSkeleton {
	public TimestampBolt() {
		setBoltName("TimestampBolt");
	}
	
	@Override
	public void boltPrepare(Map stormConf, TopologyContext context) {
		
	}

	@Override
	public void boltExecute(Tuple input, BasicOutputCollector collector) {
		List<Object> values = input.getValues();		
		values.add(System.currentTimeMillis());
		
		collector.emit(values);
	}
	
	@Override
	public void boltDeclareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("timestamp1", "timestamp2"));
	}

	@Override
	public void setGrouping(String groupingSource,
			InputDeclarer<? extends InputDeclarer> declarer) {
		declarer.shuffleGrouping(groupingSource);
		
	}
}
