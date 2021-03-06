package framework.test.storm;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

abstract public class BoltSkeleton extends BaseBasicBolt {
	static Logger log = Logger.getLogger(BoltSkeleton.class);
	
	private volatile long boltRecvCounter = 0l;
	private Timer boltRecvTimer = null;
	private String boltName = "BoltSkeleton";
	
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

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    	boltRecvTimer = new Timer();
    	boltRecvTimer.scheduleAtFixedRate(new TimerTask() {
    		public void run() {
    			log.info("current timestamp[" + System.currentTimeMillis() 
						+ "]recv count[" + boltRecvCounter + "]"); //TODO
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
