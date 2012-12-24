package framework.test.storm;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

public abstract class SpoutSkeleton extends BaseRichSpout {
	static Logger log = Logger.getLogger(SpoutSkeleton.class);
	
	private volatile long spoutEmitCounter = 0;
	private SpoutOutputCollector collector = null;
	private Timer spoutCounterTimer = null;
	private String spoutName = "SpoutSkeleton";
	private int concurrency = 1;
	
	abstract public void spoutOpen(Map conf, TopologyContext context,
			SpoutOutputCollector collector);
	
	abstract public void spoutNextTuple(SpoutOutputCollector collector);
	
	abstract public void spoutDeclareOputputFields(
			OutputFieldsDeclarer declarer);
	
	public void setSpoutName(final String name) {
		spoutName = name;
	}
	
	public String getSpoutName() {
		return spoutName;
	}
	
	public void incrEmitCounter() {
		spoutEmitCounter++;
	}
	
	public long getEmitCounter() {
		return spoutEmitCounter;
	}
	
	public void setSpoutConcurrency(int con) {
		concurrency = con;
	}
	
	public int getSpoutConcurreny() {
		return concurrency;
	}
	
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		
		spoutCounterTimer = new Timer(true);
		spoutCounterTimer.scheduleAtFixedRate(new TimerTask() {
			public void run() {
				log.info("current timestamp[" + System.currentTimeMillis() 
						+ "]emit count[" + spoutEmitCounter + "]");  //TODO
			}
		}, 0, 10000);
		
		spoutOpen(conf, context, collector);
	}

	public void nextTuple() {
		spoutNextTuple(collector);
		incrEmitCounter();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		spoutDeclareOputputFields(declarer);
	}
}
