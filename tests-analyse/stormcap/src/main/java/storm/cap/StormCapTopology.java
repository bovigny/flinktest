package storm.cap;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.json.JSONObject;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class StormCapTopology {

    public static class JSONDeserializeBolt extends BaseRichBolt {

        private static final Logger LOG = Logger.getLogger(JSONDeserializeBolt.class);
        private OutputCollector collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "timestamp", "valueKelvin"));
        }

        @Override
        public void execute(Tuple tuple) {
            // Parse JSON string
            String content = tuple.getString(0);
            LOG.info("DeserializeBolt - Received - " + content);
            JSONObject obj = new JSONObject(content).getJSONObject("mesure");

            // Emit new tuple with values parsed from JSON string
            collector.emit(tuple,
                new Values(
                    obj.getString("id"),
                    obj.getString("timestamp"),
                    Float.parseFloat(obj.getString("value"))
                )
            );

            // Acknowledge the tuple received in params
            collector.ack(tuple);
        }
    }

    public static class AugmentationBolt extends BaseRichBolt {

        private static final Logger LOG = Logger.getLogger(AugmentationBolt.class);
        private OutputCollector collector;
        private Map<String, Boolean> captorProcessEnabledMap;
        private Map<String, String> captorNameMap;

        public AugmentationBolt() {
            captorProcessEnabledMap = new HashMap<>();
            captorProcessEnabledMap.put("1/3/25", true);
            captorProcessEnabledMap.put("2/3/25", true);
            captorProcessEnabledMap.put("3/3/25", false);
            captorNameMap = new HashMap<>();
            captorNameMap.put("1/3/25", "Température 1er");
            captorNameMap.put("2/3/25", "Température 2ème");
            captorNameMap.put("3/3/25", "Température 3ème");
        }

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "timestamp", "valueKelvin", "captorName", "captorProcessEnabled"));
        }

        @Override
        public void execute(Tuple tuple) {
            String id = tuple.getStringByField("id");
            collector.emit(tuple,
                new Values(
                    id,
                    tuple.getStringByField("timestamp"),
                    tuple.getFloatByField("valueKelvin"),
                    captorNameMap.get(id),
                    captorProcessEnabledMap.get(id)
                )
            );
            collector.ack(tuple);
        }
    }

    public static class FilterProcessBolt extends BaseRichBolt {

        private static final Logger LOG = Logger.getLogger(FilterProcessBolt.class);
        private OutputCollector collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "timestamp", "valueKelvin", "captorName", "captorProcessEnabled"));
        }

        @Override
        public void execute(Tuple tuple) {
            if (tuple.getBooleanByField("captorProcessEnabled")) {
                collector.emit(tuple,
                    new Values(
                        tuple.getStringByField("id"),
                        tuple.getStringByField("timestamp"),
                        tuple.getFloatByField("valueKelvin"),
                        tuple.getStringByField("captorName"),
                        tuple.getBooleanByField("captorProcessEnabled")
                    )
                );
            }
            collector.ack(tuple);
        }
    }

    public static class ConversionBolt extends BaseRichBolt {

        private static final Logger LOG = Logger.getLogger(ConversionBolt.class);
        private OutputCollector collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "timestamp", "valueCelsius", "captorName", "captorProcessEnabled"));
        }

        @Override
        public void execute(Tuple tuple) {
            float valueCelsius = tuple.getFloatByField("valueKelvin") - (float) 273.15;
            collector.emit(tuple,
                new Values(
                    tuple.getStringByField("id"),
                    tuple.getStringByField("timestamp"),
                    valueCelsius,
                    tuple.getStringByField("captorName"),
                    tuple.getBooleanByField("captorProcessEnabled")
                )
            );
            collector.ack(tuple);
        }
    }

    public static class OutputBolt extends BaseRichBolt {

        private static final Logger LOG = Logger.getLogger(OutputBolt.class);
        private OutputCollector collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public void execute(Tuple tuple) {
            LOG.info(tuple.getStringByField("captorName") + " ; " + tuple.getStringByField("timestamp") + " ; " + tuple.getFloatByField("valueCelsius"));
            collector.ack(tuple);
        }
    }

    // Kafka: 10.10.0.21
    // Zk: 10.10.0.14

    public static void main(String[] args) throws Exception {
        String zkServerHosts = "10.10.0.14:2181";
        String kafkaTopic = "json";
        int kafkaPartitions = 1;
        int workers = 1;
        int ackers = 1;
        int cores = 4;
        int parallel = Math.max(1, cores / 7);

        // Kafka-spout
        ZkHosts hosts = new ZkHosts(zkServerHosts);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, kafkaTopic, "/" + kafkaTopic, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        // Configure topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("mesures", kafkaSpout, kafkaPartitions);
        builder.setBolt("deserializer", new JSONDeserializeBolt(), parallel).shuffleGrouping("mesures");
        builder.setBolt("augmentation", new AugmentationBolt(), parallel).shuffleGrouping("deserializer");
        builder.setBolt("filter", new FilterProcessBolt(), parallel).shuffleGrouping("augmentation");
        builder.setBolt("conversion", new ConversionBolt(), parallel).shuffleGrouping("filter");
        builder.setBolt("output", new OutputBolt(), parallel).shuffleGrouping("conversion");

        // Create and deploy the topology
        Config conf = new Config();
        conf.setNumWorkers(workers);
        conf.setNumAckers(ackers);
        StormSubmitter.submitTopologyWithProgressBar("stormcap-topology", conf, builder.createTopology());
    }
}
