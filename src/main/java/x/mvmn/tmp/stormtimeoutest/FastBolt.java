package x.mvmn.tmp.stormtimeoutest;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class FastBolt extends BaseRichBolt {
	private static final long serialVersionUID = -98497149556258503L;

	protected Map<?, ?> stormConfig;
	protected TopologyContext topologyContext;
	protected OutputCollector outputCollector;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConfig, TopologyContext topologyContext, OutputCollector outputCollector) {
		System.err.println("Fast bolt preparing");
		this.stormConfig = stormConfig;
		this.topologyContext = topologyContext;
		this.outputCollector = outputCollector;

	}

	@Override
	public void execute(Tuple tuple) {
		String payload = new String(tuple.getBinary(0), StandardCharsets.UTF_8);
		System.out.println("Fast bolt called with tuple: " + tuple + " / " + payload);
		outputCollector.emit(tuple, new Values("Fast bolt was here. " + payload));
		outputCollector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		System.err.println("Fast bolt declaring fields");
		outputFieldsDeclarer.declare(new Fields("payload"));
	}
}
