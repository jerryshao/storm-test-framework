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
	protected TopologyBuilder builder = new TopologyBuilder();
	private int spoutCon = 1;
	private int boltCon = 1;

	abstract public void initTopologyBuilder(final String[] args);
	
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
	
	public void setSpoutCon(final int con) {
		spoutCon = con;
	}
	
	public int getSpoutCon() {
		return spoutCon;
	}
	
	public void setBoltCon(final int con) {
		boltCon = con;
	}
	
	public int getBoltCon() {
		return boltCon;
	}
	
	public static void main(String[] args) {
		if (args == null || args.length < 2) {
			System.out.println("Usage: TopologySkeleton topology local|remote name " +
					"[thread time|worker num] [spout concurrency] [bolt concurrency] " +
					"[specific topology parameter]");
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
			log.error(e);
			return;
		}
		topology.setTopologyName(topologyName);
		
		if (args.length > 3) {
			topology.setSpoutCon(Integer.parseInt(args[3]));
		}
		
		if (args.length > 4) {
			topology.setBoltCon(Integer.parseInt(args[4]));
		}
		
		String[] specificArgs = null;
		if (args.length  > 5) {
			specificArgs = new String[args.length - 5];
			System.arraycopy(args, 5, specificArgs, 0, specificArgs.length);
		}
		
		topology.initTopologyBuilder(specificArgs);

		try {
			if (args[0].equals("local")) {
				int sleep = 30000;
				if (args.length > 2) {
					sleep = Integer.parseInt(args[2]);
				}
				
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology(topology.getTopologyName(), topology.getConfig(),
						topology.getTopologyBuilder().createTopology());
		
				Thread.sleep(sleep);
				cluster.shutdown();
			} else if (args[0].equals("remote")) {
				if (args.length > 2) {
					topology.getConfig().setNumWorkers(Integer.parseInt(args[2]));
				} else {
					topology.getConfig().setNumWorkers(1);
				}
				
				topology.getConfig().setMaxSpoutPending(5000);
				
				StormSubmitter.submitTopology(topology.getTopologyName(),
						topology.getConfig(), 
						topology.getTopologyBuilder().createTopology());	
			} else {
				throw new InvalidParameterException("unknown parameter " + args[0]);
			}
		} catch (Exception e) {
			log.error(e);
		}
	}
}
