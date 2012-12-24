package framework.test.storm.topology;

import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.TopologyBuilder;
import framework.test.storm.TopologySkeleton;
import framework.test.storm.bolt.WordGrepBolt;
import framework.test.storm.spout.WordGrepSpout;

public class WordGrepTopology extends TopologySkeleton {

	@Override
	public void setTopologyBuilder(TopologyBuilder builder) {
		WordGrepSpout spout = new WordGrepSpout();
		builder.setSpout(spout.getSpoutName(), spout, 
				spout.getSpoutConcurreny());
		
		WordGrepBolt bolt = new WordGrepBolt();
		bolt.setGrepWord("dfafda");
		
		InputDeclarer<? extends InputDeclarer> de = 
				builder.setBolt(bolt.getBoltName(), bolt, 
						bolt.getBoltConcurrency());
		bolt.setGrouping(spout.getSpoutName(), de);		
	}
}
