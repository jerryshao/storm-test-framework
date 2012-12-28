package framework.test.storm.bolt;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import framework.test.storm.BoltSkeleton;

public class TimestampSinkBolt extends BoltSkeleton {
	static Logger log = Logger.getLogger(TimestampSinkBolt.class);

	public TimestampSinkBolt() {
		setBoltName("TimestampSinkBolt");
	}
	
	@Override
	public void boltPrepare(Map stormConf, TopologyContext context) {
		
	}

	@Override
	public void boltExecute(Tuple input, BasicOutputCollector collector) {
		List<Object> values = input.getValues();
		
		if (getRecvCounter() % 100 == 0) {
			// sample every 100 tuple
			List<Long> latencyList = new ArrayList<Long>();
			Iterator<Object> it = values.iterator();		
			while (it.hasNext()) {
				long first = (Long)it.next();
				long second = 0;
				if (it.hasNext())  {
					second = (Long)it.next();
				} else {
					break;
				}
				
				long latency = second - first;
				latencyList.add(latency);
			}
			
			StringBuilder sb = new StringBuilder();
			for (long i : latencyList) {
				sb.append("message latency[" + i + "]");
			}
			log.info(sb.toString());
		}
		
		collector.emit(values);
	}
	
	@Override
	public void boltDeclareOutputFields(OutputFieldsDeclarer declarer) {	
		declarer.declare(new Fields("timestamp1", "timestamp2"));
	}

	@Override
	public void setGrouping(String groupingSource,
			InputDeclarer<? extends InputDeclarer> declarer) {
		declarer.shuffleGrouping(groupingSource);
	}
		
}
