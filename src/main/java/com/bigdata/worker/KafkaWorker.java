package com.bigdata.worker;

import com.bigdata.model.Uservisit;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

import static com.google.common.collect.Streams.stream;
import static java.util.Collections.singletonList;

public class KafkaWorker {

    private static final String TABLE_NAME = "uservisits";
    private static final String IP_ADRESS = "127.0.0.1";

    public void run() {

        Consumer<String,String> kafkaConsumer = createKafkaConsumer();
        Cluster cassandraCluster = Cluster.builder().addContactPoint(IP_ADRESS).build();
        Session session = cassandraCluster.connect();

        MappingManager manager = new MappingManager(session);
        Mapper<Uservisit> mapper = manager.mapper(Uservisit.class);

        while (true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
            stream(records)
                    .map(ConsumerRecord::value)
                    .map(Uservisit::buildUservisitModelFromString)
                    .forEach(mapper::save);
        }
    }

    private Consumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "run");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(singletonList(TABLE_NAME));
        return kafkaConsumer;
    }

}
