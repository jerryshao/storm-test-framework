package framework.test.storm.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import framework.test.storm.BoltSkeleton;

public class TimestampBolt extends BoltSkeleton {
	private int valueNum = 0;

	@Override
	public void boltPrepare(Map stormConf, TopologyContext context) {
		setBoltName("TimestampBolt");
	}

	@Override
	public void boltExecute(Tuple input, BasicOutputCollector collector) {
		List<Object> values = input.getValues();
		values.add(System.currentTimeMillis());
		valueNum = values.size();
		
		collector.emit(values);
	}
	
	@Override
	public void boltDeclareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> fields = new ArrayList<String>(valueNum);
		for (int i = 0; i < valueNum; i++) {
			fields.add("timestamp");
		}
		
		declarer.declare(new Fields(fields));
	}
}
