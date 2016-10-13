package spark.cap;

import org.apache.spark.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.*;
import org.json.JSONObject;
import org.apache.log4j.Logger;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;

import java.util.HashMap;
import java.util.Map;

public class SparkCap {

    private static final Logger LOG = Logger.getLogger(SparkCap.class);

    public static void main(String[] args) throws Exception {
        String zkServerHosts = "10.10.0.14:2181";
        String kafkaTopic = "json";
        int workers = 1;
        String group = "default";

        final Map<String, Boolean> captorProcessEnabledMap = new HashMap<>();
        captorProcessEnabledMap.put("1/3/25", true);
        captorProcessEnabledMap.put("2/3/25", true);
        captorProcessEnabledMap.put("3/3/25", false);
        final Map<String, String> captorNameMap = new HashMap<>();
        captorNameMap.put("1/3/25", "Température 1er");
        captorNameMap.put("2/3/25", "Température 2ème");
        captorNameMap.put("3/3/25", "Température 3ème");

        SparkConf sparkConf = new SparkConf().setAppName("sparkcap");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000)); // Create the context with 2 seconds batch size

        // Kafka DSTream
        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put(kafkaTopic, workers);
        JavaPairReceiverInputDStream<String, String> mesures = KafkaUtils.createStream(jssc, zkServerHosts, group, topicMap);

        // Deserialize
        JavaDStream<Tuple3<String, String, Float>> deserializedMesures = mesures.map(new Function<Tuple2<String, String>, Tuple3<String, String, Float>>() {
            @Override
            public Tuple3<String, String, Float> call(Tuple2<String, String> tuple) {
                String content = tuple._2();
                LOG.info("Deserializer - Received - " + content);
                JSONObject obj = new JSONObject(content).getJSONObject("mesure");

                // Emit new tuple with values parsed from JSON string
                return new Tuple3<>(obj.getString("id"), obj.getString("timestamp"), Float.parseFloat(obj.getString("value")));
            }
        });

        // Augmentation
        JavaDStream<Tuple5<String, String, Float, String, Boolean>> augmentedMesures = deserializedMesures.map(new Function<Tuple3<String, String, Float>, Tuple5<String, String, Float, String, Boolean>>() {
            @Override
            public Tuple5<String, String, Float, String, Boolean> call(Tuple3<String, String, Float> tuple) {
                String id = tuple._1();
                return new Tuple5<>(id, tuple._2(), tuple._3(), captorNameMap.get(id), captorProcessEnabledMap.get(id));
            }
        });

        // Filter
        JavaDStream<Tuple5<String, String, Float, String, Boolean>> filteredMesures = augmentedMesures.filter(new Function<Tuple5<String, String, Float, String, Boolean>, Boolean>() {
            @Override
            public Boolean call(Tuple5<String, String, Float, String, Boolean> tuple) {
                return tuple._5();
            }
        });

        // Conversion
        JavaDStream<Tuple5<String, String, Float, String, Boolean>> convertedMesures = filteredMesures.map(new Function<Tuple5<String, String, Float, String, Boolean>, Tuple5<String, String, Float, String, Boolean>>() {
            @Override
            public Tuple5<String, String, Float, String, Boolean> call(Tuple5<String, String, Float, String, Boolean> tuple) {
                float valueCelsius = tuple._3() - (float) 273.15;
                return new Tuple5<>(tuple._1(), tuple._2(), valueCelsius, tuple._4(), tuple._5());
            }
        });

        // Output
        convertedMesures.map(new Function<Tuple5<String,String,Float,String,Boolean>, String>() {
            @Override
            public String call(Tuple5<String, String, Float, String, Boolean> tuple) {
                String compact = tuple._4() + " ; " + tuple._2() + " ; " + tuple._3();
                LOG.info("LOG: " + compact);
                return compact;
            }
        }).print();

        // Create and deploy application
        jssc.start();
        jssc.awaitTermination();
    }
}
