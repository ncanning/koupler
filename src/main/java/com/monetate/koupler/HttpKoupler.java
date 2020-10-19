package com.monetate.koupler;

import static spark.Spark.post;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.monetate.koupler.format.SplitFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class HttpKoupler implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpKoupler.class);

    KinesisProducer theProducer;

    public HttpKoupler(int port, String propertiesFile, KinesisProducer producer) {
        KinesisProducerConfiguration config = KinesisProducerConfiguration.fromPropertiesFile(propertiesFile);
        if (producer == null) {
            theProducer = new KinesisProducer(config);
        } else {
            theProducer = producer;
        }
        LOGGER.info("Firing up HTTP listener on [{}]", port);
    }
    
    @Override
    public void run() {
        post("/:stream", (request, response) -> {
            String streamName = request.params(":stream");
            String msg = request.body();
            String partitionKey = msg.split(",", 2)[0];
            String data = msg.split(",", 2)[1];

            byte[] bytes = data.getBytes("UTF-8");
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            LOGGER.info("request body: " + data);
            LOGGER.info("request partition key: " + partitionKey);
            theProducer.addUserRecord(streamName, partitionKey, buffer);
            return "ACK\n";
        });
    }
}
