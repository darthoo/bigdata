package com.bigdata;

import com.bigdata.worker.KafkaWorker;

public class Main {

    public static void main(String[] args) {
        KafkaWorker kafkaWorker = new KafkaWorker();
        kafkaWorker.run();
    }
}
