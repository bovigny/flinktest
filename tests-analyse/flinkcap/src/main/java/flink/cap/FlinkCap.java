package flink.cap;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class FlinkCap {

    private static final Logger LOG = Logger.getLogger(FlinkCap.class);

    public static void main(String[] args) throws Exception {
        String zkServerHosts = "10.10.0.14:2181";
        String kafkaBroker = "10.10.0.21:6667";
        String kafkaTopic = "json";
        String group = "default";

        final Map<String, Boolean> captorProcessEnabledMap = new HashMap<>();
        captorProcessEnabledMap.put("1/3/25", true);
        captorProcessEnabledMap.put("2/3/25", true);
        captorProcessEnabledMap.put("3/3/25", false);
        final Map<String, String> captorNameMap = new HashMap<>();
        captorNameMap.put("1/3/25", "Température 1er");
        captorNameMap.put("2/3/25", "Température 2ème");
        captorNameMap.put("3/3/25", "Température 3ème");

        // set up the execution environment
        Configuration conf = new Configuration();
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // DataStream from Kafka
        Properties prop = new Properties();
        prop.put("zookeeper.connect", zkServerHosts);
        prop.put("group.id", group);
        prop.put("bootstrap.servers", kafkaBroker);
        DataStream<String> mesures = env.addSource(new FlinkKafkaConsumer08<>(kafkaTopic, new SimpleStringSchema(), prop));

        // Deserialize
        DataStream<Tuple3<String, String, Float>> deserializedMesures = mesures.map(new MapFunction<String, Tuple3<String, String, Float>>() {
            @Override
            public Tuple3<String, String, Float> map(String s) {
                LOG.info("Deserializer - Received - " + s);
                JSONObject obj = new JSONObject(s).getJSONObject("mesure");

                // Emit new tuple with values parsed from JSON string
                return new Tuple3<>(obj.getString("id"), obj.getString("timestamp"), Float.parseFloat(obj.getString("value")));
            }
        });

        // Augmentation
        DataStream<Tuple5<String, String, Float, String, Boolean>> augmentedMesures = deserializedMesures.map(new MapFunction<Tuple3<String, String, Float>, Tuple5<String, String, Float, String, Boolean>>() {
            @Override
            public Tuple5<String, String, Float, String, Boolean> map(Tuple3<String, String, Float> tuple) {
                String id = tuple.f0;
                return new Tuple5<>(id, tuple.f1, tuple.f2, captorNameMap.get(id), captorProcessEnabledMap.get(id));
            }
        });

        // Filter
        DataStream<Tuple5<String, String, Float, String, Boolean>> filteredMesures = augmentedMesures.filter(new FilterFunction<Tuple5<String,String,Float,String,Boolean>>() {
            @Override
            public boolean filter(Tuple5<String, String, Float, String, Boolean> tuple) {
                return tuple.f4;
            }
        });

        // Conversion
        DataStream<Tuple5<String, String, Float, String, Boolean>> convertedMesures = filteredMesures.map(new MapFunction<Tuple5<String, String, Float, String, Boolean>, Tuple5<String, String, Float, String, Boolean>>() {
            @Override
            public Tuple5<String, String, Float, String, Boolean> map(Tuple5<String, String, Float, String, Boolean> tuple) {
                float valueCelsius = tuple.f2 - (float) 273.15;
                return new Tuple5<>(tuple.f0, tuple.f1, valueCelsius, tuple.f3, tuple.f4);
            }
        });

        // Output
        convertedMesures.map(new MapFunction<Tuple5<String,String,Float,String,Boolean>, String>() {
            @Override
            public String map(Tuple5<String, String, Float, String, Boolean> tuple) {
                String compact = tuple.f3 + " ; " + tuple.f1 + " ; " + tuple.f2;
                LOG.info("LOG: " + compact);
                return compact;
            }
        }).print();

        env.execute();
    }
}
