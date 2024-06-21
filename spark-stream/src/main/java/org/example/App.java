package org.example;

import com.datastax.oss.driver.shaded.json.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;


public class App 
{
    public static void main( String[] args ) throws InterruptedException {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("StreamingApp");
        sparkConf.setMaster("local[*]");
        sparkConf.set("spark.cassandra.connection.host", "127.0.0.1");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(10));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "streamer5ee");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = Arrays.asList("create_user");

        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));

        JavaPairDStream<String, String> results = messages.mapToPair(
                        record -> new Tuple2<>(record.key(), record.value())
                );


        results.foreachRDD(rdd->{
            List<Users> users = new ArrayList<>();
            System.out.println("New Data Arrived: "+rdd.partitions().size() +"partitions " + rdd.count() + "records");
            if(rdd.count()>0){
                rdd.collect().forEach(rawRecord ->{
                    System.out.println(rawRecord);
                    System.out.println("******************");
                    System.out.println(rawRecord._2);
                    String record = rawRecord._2();
                    JSONObject jsonObject = new JSONObject(record);


                    String first_name = jsonObject.getString("first_name");
                    String last_name = jsonObject.getString("last_name");
                    String gender = jsonObject.getString("gender");
                    String address = jsonObject.getString("address");
                    String post_code = jsonObject.get("postcode").toString();
                    String dob = jsonObject.getString("dob");
                    String email = jsonObject.getString("email");
                    String username = jsonObject.getString("username");
                    String password = jsonObject.getString("password");
                    String registered_date = jsonObject.getString("registered_date");
                    String phone = jsonObject.getString("phone");
                    String picture = jsonObject.getString("picture");
                    String user_id = UUID.randomUUID().toString();
                    Users user = new Users(user_id,first_name,last_name,gender,address,post_code,dob,email,username,password,registered_date,phone,picture);
                    users.add(user);
                });
                JavaRDD<Users> refinedRDD = streamingContext.sparkContext().parallelize(users);
                javaFunctions(refinedRDD).writerBuilder("user_data","users",mapToRow(Users.class)).saveToCassandra();
            }
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
