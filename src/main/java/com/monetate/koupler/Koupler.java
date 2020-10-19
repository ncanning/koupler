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

    public static void main(String[] args) throws ParseException {
        Options options = new Options();

        options.addOption("http", false, "http mode");

        String propertiesFile = "./conf/kpl.properties";
        options.addOption("propertiesFile", true, "kpl properties file (default: " + propertiesFile + ")");

        int port = 4242;
        options.addOption("port", true, "listening port (default: " + port + ")");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("propertiesFile")) {
            propertiesFile = cmd.getOptionValue("propertiesFile");
        }
        if (cmd.hasOption("port")) {
            port = Integer.parseInt(cmd.getOptionValue("port"));
        }

        HttpKoupler koupler = new HttpKoupler(port, propertiesFile, null);

        Thread kouplerThread = new Thread(koupler);
        kouplerThread.start();
    }
}
