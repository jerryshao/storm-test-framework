package framework.test.storm.topology;

import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.TopologyBuilder;
import framework.test.storm.TopologySkeleton;
import framework.test.storm.bolt.TimestampBolt;
import framework.test.storm.bolt.TimestampSinkBolt;
import framework.test.storm.spout.TimestampSpout;

public class TimestampTopology extends TopologySkeleton {

	@Override
	public void initTopologyBuilder(final String[] args) {
		TimestampSpout spout = new TimestampSpout();
		spout.setSpoutConcurrency(getSpoutCon());
		builder.setSpout(spout.getSpoutName(), spout, 
				spout.getSpoutConcurreny());
		
		TimestampBolt bolt = new TimestampBolt();
		bolt.setBoltConcurrency(getBoltCon());
		InputDeclarer<? extends InputDeclarer> de = 
				builder.setBolt(bolt.getBoltName(), bolt, 
						bolt.getBoltConcurrency());
		bolt.setGrouping(spout.getSpoutName(), de);
		
		TimestampSinkBolt sinkBolt = new TimestampSinkBolt();
		sinkBolt.setBoltConcurrency(1);
		InputDeclarer<? extends InputDeclarer> d = 
				builder.setBolt(sinkBolt.getBoltName(), sinkBolt, 
						sinkBolt.getBoltConcurrency());
		sinkBolt.setGrouping(bolt.getBoltName(), d);
	}
}
