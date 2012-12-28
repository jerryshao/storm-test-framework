package framework.test.storm;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

abstract public class BoltSkeleton extends BaseBasicBolt {
	static Logger log = Logger.getLogger(BoltSkeleton.class);
	
	private volatile long boltRecvCounter = 0l;
	private Timer boltRecvTimer = null;
	private String boltName = "BoltSkeleton";
	private int concurrency = 1;
	
	abstract public void boltPrepare(Map stormConf, TopologyContext context);
	
	abstract public void boltExecute(Tuple input, BasicOutputCollector collector);
	
	abstract public void boltDeclareOutputFields(OutputFieldsDeclarer declarer);
	
	public void setBoltName(final String name) {
		boltName = name;
	}
	
	public String getBoltName() {
		return boltName;
	}
	
	public void incrRecvCounter() {
		boltRecvCounter++;
	}
	
	public long getRecvCounter() {
		return boltRecvCounter;
	}
	
	public void setBoltConcurrency(final int con) { 
		concurrency = con;
	}
	
	public int getBoltConcurrency() {
		return concurrency;
	}
	
	public void setGrouping(String groupingSource, 
			InputDeclarer<? extends InputDeclarer> declarer) {
		declarer.shuffleGrouping(groupingSource);
	}

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    	boltRecvTimer = new Timer(true);
    	boltRecvTimer.scheduleAtFixedRate(new TimerTask() {
    		private long lastCount = 0;
    		public void run() {
    			log.info("recv count/s[" + (boltRecvCounter - lastCount) / 10 + "]");
    			lastCount = boltRecvCounter;
    		}
    	}, 0, 10000);
    	
    	boltPrepare(stormConf, context);
    }
    
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		boltExecute(input, collector);
		incrRecvCounter();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		boltDeclareOutputFields(declarer);
	}
}
