package framework.test.storm;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class TopologySkeleton {
	static Logger log = Logger.getLogger(TopologySkeleton.class);
	
	public static void main(String[] args) throws InterruptedException {
		if (args == null || args.length == 0) {
			log.info("Usage: TopologySkeleton topology name");
			return;
		}
		
		String spoutName = "framework.test.storm.spout." + args[0] + "Spout";
		String boltName = "framework.test.storm.bolt." + args[0] + "Bolt";

		SpoutSkeleton spout = null;
		BoltSkeleton bolt = null;
		try {
			Class<?> spoutClass = Class.forName(spoutName);
			Class<?> boltClass = Class.forName(boltName);
			
			spout = (SpoutSkeleton) spoutClass.newInstance();
			bolt = (BoltSkeleton) boltClass.newInstance();
		} catch (Exception e) {
			log.error(e.getMessage());
		}
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(spout.getSpoutName(), spout);
		builder.setBolt(bolt.getBoltName(), bolt).shuffleGrouping(spout.getSpoutName());
		
		// for test, TODO
		Config conf = new Config();
	    conf.setMaxTaskParallelism(3);
		
	    LocalCluster cluster = new LocalCluster();
	    cluster.submitTopology(args[0], conf, builder.createTopology());
	
	    Thread.sleep(3000);
	
	    cluster.shutdown();

	}

}
