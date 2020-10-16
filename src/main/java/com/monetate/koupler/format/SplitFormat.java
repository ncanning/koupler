package com.monetate.koupler.format;

import org.apache.commons.cli.CommandLine;

/**
 * Created by brianoneill on 4/29/16.
 */
public class SplitFormat implements Format {
    private int streamNameField = 0;
    private int partitionKeyField = 1;
    private int dataField = 2;
    private String delimiter = ",";

    /**
     * Constructor for Split format
     */
    public SplitFormat(CommandLine cmd){
        if (cmd.hasOption("delimiter")) {
            delimiter = cmd.getOptionValue("delimiter");
        }
    }

    @Override
    public String getStreamName(String event) {
        return event.split(this.delimiter, 3)[streamNameField];
    }

    @Override
    public String getPartitionKey(String event) {
        return event.split(this.delimiter, 3)[partitionKeyField];
    }

    @Override
    public String getData(String event) {
        return event.split(this.delimiter, 3)[dataField];
    }
}
