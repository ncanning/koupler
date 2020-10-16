package com.monetate.koupler;

import static spark.Spark.post;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpKoupler extends Koupler implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpKoupler.class);

    KinesisEventProducer producer = new KinesisEventProducer(format, cmd, propertiesFile, queueSize, appName);

    public HttpKoupler(int port) {
        super(20);
        LOGGER.info("Firing up HTTP listener on [{}]", port);
    }
    
    @Override
    public void run() {
        post("/:stream", (request, response) -> {
            String event = request.body();
            LOGGER.info("request body: " + event);
            getOrCreateProducer(request.params(":stream")).queueEvent(event);
            return "ACK\n";
        });
    }
}
