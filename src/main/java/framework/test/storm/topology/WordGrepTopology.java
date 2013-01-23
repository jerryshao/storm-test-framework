package framework.test.storm.topology;

import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.TopologyBuilder;
import framework.test.storm.TopologySkeleton;
import framework.test.storm.bolt.WordGrepBolt;
import framework.test.storm.spout.WordGrepSpout;

public class WordGrepTopology extends TopologySkeleton {

	@Override
	public void initTopologyBuilder(final String[] args) {
		WordGrepSpout spout = new WordGrepSpout();
		spout.setSpoutConcurrency(getSpoutCon());
		if (args != null && args.length > 0) {
			spout.setSentenceLen(Integer.parseInt(args[0]));
		} else {
			spout.setSentenceLen(100);
		}
		
		builder.setSpout(spout.getSpoutName(), spout, 
				spout.getSpoutConcurreny());
		
		WordGrepBolt bolt = new WordGrepBolt();
		bolt.setBoltConcurrency(getBoltCon());
		if (args != null && args.length > 1) {
			bolt.setGrepWord(args[1]);
		} else {
			bolt.setGrepWord("qweasdzxcrtyfghv");
		}
		
		InputDeclarer<? extends InputDeclarer> de = 
				builder.setBolt(bolt.getBoltName(), bolt, 
						bolt.getBoltConcurrency());
		bolt.setGrouping(spout.getSpoutName(), de);		
	}
}
