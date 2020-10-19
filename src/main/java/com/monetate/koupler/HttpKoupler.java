package com.monetate.koupler;

import static spark.Spark.post;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class HttpKoupler implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpKoupler.class);

    KinesisProducer producer;

    public HttpKoupler(int port, String propertiesFile, KinesisProducer producer) {
        KinesisProducerConfiguration config = KinesisProducerConfiguration.fromPropertiesFile(propertiesFile);
        if (producer == null) {
            this.producer = new KinesisProducer(config);
        } else {
            this.producer = producer;
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
            producer.addUserRecord(streamName, partitionKey, buffer.asReadOnlyBuffer());
            return "ACK\n";
        });
    }
}
