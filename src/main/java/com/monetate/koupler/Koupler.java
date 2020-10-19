package com.monetate.koupler;

import java.io.BufferedReader;
import java.net.SocketException;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main Class, and super class to all the koupler implementations.
 *
 * @author brianoneill
 */
public class Koupler {

    public KinesisEventProducer producer;
    private ExecutorService threadPool;

    public Koupler(KinesisEventProducer producer, int threadPoolSize) {
        this.producer = producer;
        this.threadPool = Executors.newFixedThreadPool(threadPoolSize);
    }

    public static void main(String[] args) throws ParseException {
        boolean misconfigured = false;
        Options options = new Options();

        String propertiesFile = "./conf/kpl.properties";
        options.addOption("propertiesFile", true, "kpl properties file (default: " + propertiesFile + ")");

        int port = 4242;
        options.addOption("port", true, "listening port (default: " + port + ")");
        options.addOption("partitionKeyField", true,
                "field containing partition key (default: " + 0 + ")");

        options.addOption("format", true, "format of data (default: 'split')");
        options.addOption("delimiter", true, "delimiter between fields (default: ',')");
        options.addOption("udp", false, "udp mode");
        options.addOption("http", false, "http mode");
        options.addOption("tcp", false, "tcp mode");
        options.addOption("pipe", false, "pipe mode");
        options.addOption("consumer", false, "consumer mode");
        options.addOption("streamName", true, "kinesis stream name");
        options.addOption("appName", true, "app/consumer name");
        options.addOption("position", true, "initial position in stream (default: LATEST)");
        options.addOption("metrics", false, "publish metrics to cloudwatch");
        options.addOption("queueSize", true, "event buffer/queue size (default: 50000)");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("propertiesFile")) {
            propertiesFile = cmd.getOptionValue("propertiesFile");
        }
        if (cmd.hasOption("port")) {
            port = Integer.parseInt(cmd.getOptionValue("port"));
        }

        // Check to see they specified one of (udp, tcp http, or pipe)
        if (!cmd.hasOption("udp") && !cmd.hasOption("tcp") && !cmd.hasOption("http") && !cmd.hasOption("pipe")
                && !cmd.hasOption("consumer")) {
            System.err.println("Must specify either: udp, http, tcp, pipe, or consumer");
            misconfigured = true;
        }

        String streamName = "";
        if (cmd.hasOption("streamName")) {
            streamName = cmd.getOptionValue("streamName");
        }

        int queueSize = 50000;
        if (cmd.hasOption("queueSize")) {
            queueSize = Integer.parseInt(cmd.getOptionValue("queueSize"));
        }

        if (misconfigured) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp("java -jar koupler*.jar", options);
            System.exit(-1);
        }

        String appName = "koupler";
        if (cmd.hasOption("appName")) {
            appName = cmd.getOptionValue("appName");
        }

        String format = "split";
        if (cmd.hasOption("format")) {
            format = cmd.getOptionValue("format");
        }

        KinesisEventProducer producer = new KinesisEventProducer(format, cmd, propertiesFile, queueSize, appName);
        if (cmd.hasOption("metrics")) {
            producer.startMetrics();
        }

        HttpKoupler koupler = new HttpKoupler(port, propertiesFile, null);

        Thread kouplerThread = new Thread(koupler);
        kouplerThread.start();
    }

}
