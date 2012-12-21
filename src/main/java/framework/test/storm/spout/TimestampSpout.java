package framework.test.storm.spout;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import framework.test.storm.SpoutSkeleton;

public class TimestampSpout extends SpoutSkeleton {

	@Override
	public void spoutOpen(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		setSpoutName("TimestampSpout");
	}

	@Override
	public void spoutNextTuple(SpoutOutputCollector collector) {
		collector.emit(new Values(System.currentTimeMillis()));
		
	}

	@Override
	public void spoutDeclareOputputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("timestamp1"));
	}
}
