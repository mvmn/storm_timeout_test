package x.mvmn.tmp.stormtimeoutest;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;

public class StormTopologyBuilder {

	public static void main(String[] args) throws Exception {
		Map<String, String> argsAsMap = new HashMap<>();
		for (String arg : args) {
			int indexOfEq = arg.indexOf("=");
			String key = arg;
			String value = "true";
			if (indexOfEq > 0) {
				key = arg.substring(0, indexOfEq);
				value = arg.substring(indexOfEq + 1);
			}
			argsAsMap.put(key, value);
		}

		final String topologyName = "MvmnStormTestTopology";
		Config config = new Config();

		config.setDebug(Boolean.valueOf(strProp("topology.debug", "true", argsAsMap)));

		config.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, intProp("topology.timeout", "30", argsAsMap));

		config.put(Config.NIMBUS_TASK_TIMEOUT_SECS, intProp("topology.task.timeout", "30", argsAsMap));
		config.put(Config.NIMBUS_SUPERVISOR_TIMEOUT_SECS, intProp("topology.supervisor.timeout", "60", argsAsMap));

		config.put(Config.SUPERVISOR_WORKER_START_TIMEOUT_SECS, intProp("topology.worker.start.timeout", "120", argsAsMap));
		config.put(Config.SUPERVISOR_WORKER_TIMEOUT_SECS, intProp("topology.worker.timeout", "30", argsAsMap));

		config.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, intProp("topology.buffer.transfer", "1024", argsAsMap));
		config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, intProp("topology.buffer.receive", "1024", argsAsMap));
		config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, intProp("topology.buffer.send", "1024", argsAsMap));

		config.put("xmvmn.slowBoltWaitTime", "10000");

		TopologyBuilder topologyBuilder = new TopologyBuilder();

		SpoutConfig spoutConfig = new SpoutConfig(new ZkHosts(strProp("zk.host", "localhost", argsAsMap)),
				strProp("kafka.topic", "stormTimeoutsTestTopic", argsAsMap), strProp("zk.root", "", argsAsMap),
				strProp("kafka.spout.id", "testKafkaSpout", argsAsMap));

		topologyBuilder.setSpout("kafkaSpout", new KafkaSpout(spoutConfig), intProp("kafka.spout.ph", "5", argsAsMap));
		topologyBuilder.setBolt("fastBolt", new FastBolt(), intProp("bolt.fast.ph", "10", argsAsMap)).setNumTasks(intProp("bolt.fast.tasks", "20", argsAsMap))
				.shuffleGrouping("kafkaSpout");
		topologyBuilder.setBolt("slowBolt", new SlowBolt(), intProp("bolt.slow.ph", "100", argsAsMap)).setNumTasks(intProp("bolt.slow.tasks", "200", argsAsMap))
				.shuffleGrouping("kafkaSpout");

		if (Boolean.valueOf(System.getProperty("localStormCluster", "false"))) {
			new LocalCluster().submitTopology(topologyName, config, topologyBuilder.createTopology());
		} else {
			StormSubmitter.submitTopology(topologyName, config, topologyBuilder.createTopology());
		}
	}

	protected static String strProp(String name, String defaultValue, Map<String, String> props) {
		return props.get(name) != null ? props.get(name) : defaultValue;
	}

	protected static int intProp(String name, String defaultVal, Map<String, String> props) {
		return Integer.parseInt(strProp(name, defaultVal, props));
	}
}
