package framework.test.storm;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.TopologyBuilder;

abstract public class TopologySkeleton {
	static Logger log = Logger.getLogger(TopologySkeleton.class);
	
	private String topologyName = "default";
	private Config conf = new Config();
	private TopologyBuilder builder = new TopologyBuilder();
	
	abstract public void setTopologyBuilder(TopologyBuilder builder);
	
	public void setTopologyName(final String name) {
		topologyName = name;
	}
	
	public String getTopologyName() {
		return topologyName;
	}
	
	public Config getConfig() {
		return conf;
	}
	
	public TopologyBuilder getTopologyBuilder() {
		return builder;
	}
	
	public static void main(String[] args) {
		if (args == null || args.length < 2) {
			System.out.println("Usage: TopologySkeleton topology local|remote name");
			return;
		}
		
		String topologyName = args[1];
		String topologyClassName = 
				"framework.test.storm.topology." + args[1] + "Topology";

		TopologySkeleton topology = null;
		try {
			Class<?> topologyClass = Class.forName(topologyClassName);
			
			topology = (TopologySkeleton) topologyClass.newInstance();
		} catch (Exception e) {
			log.error(e.getMessage()); //TODO
			return;
		}
		topology.setTopologyName(topologyName);
		topology.setTopologyBuilder(topology.getTopologyBuilder());
	
		
		try {
			if (args[0].equals("local")) {	
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology(topology.getTopologyName(), topology.getConfig(),
						topology.getTopologyBuilder().createTopology());
		
				Thread.sleep(30000);
				cluster.shutdown();
			} else if (args[0].equals("remote")) {
				if (args.length > 2) {
					topology.getConfig().setNumWorkers(Integer.parseInt(args[2]));
				}
				
				StormSubmitter.submitTopology(topology.getTopologyName(),
						topology.getConfig(), 
						topology.getTopologyBuilder().createTopology());	
			} else {
				throw new InvalidParameterException("unknow parameter " + args[0]);
			}
		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}
}
