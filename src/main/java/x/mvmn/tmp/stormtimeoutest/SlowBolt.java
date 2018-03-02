package x.mvmn.tmp.stormtimeoutest;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class SlowBolt extends BaseRichBolt {
	private static final long serialVersionUID = -98497149556258503L;

	protected Map<?, ?> stormConfig;
	protected TopologyContext topologyContext;
	protected OutputCollector outputCollector;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConfig, TopologyContext topologyContext, OutputCollector outputCollector) {
		System.err.println("Slow bolt preparing");
		this.stormConfig = stormConfig;
		this.topologyContext = topologyContext;
		this.outputCollector = outputCollector;

	}

	@Override
	public void execute(Tuple tuple) {
		String payload = tuple.getString(0);
		System.out.println("Slow bolt called with tuple: " + tuple + " / " + payload);
		try {
			int waitTime = 10000;
			if (stormConfig.get("xmvmn.slowBoltWaitTime") != null) {
				waitTime = Integer.parseInt(stormConfig.get("xmvmn.slowBoltWaitTime").toString());
			}
			System.out.println("Slow bolt waiting for " + waitTime + ". Tuple " + tuple + " / " + payload);
			Thread.sleep(waitTime);
		} catch (InterruptedException e) {
			System.err.println("Slow bolt interrupted while waiting.");
			Thread.currentThread().interrupt();
		}
		System.out.println("Slow bolt done waiting. Tuple: " + tuple + " / " + payload);
		outputCollector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		System.err.println("Slow bolt declaring fields");
	}
}
