package com.monetate.koupler.format;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import org.apache.commons.cli.CommandLine;

/**
 * Created by jpalladino on 8/20/16.
 */
public class JsonFormat implements Format {

    private String streamNameField;
    private String partitionKeyField;

    public JsonFormat(CommandLine cmd) {
        if (cmd.hasOption("partitionKeyField")) {
            partitionKeyField = cmd.getOptionValue("partitionKeyField");
        }
        if (cmd.hasOption("streamNameField")) {
            streamNameField = cmd.getOptionValue("streamNameField");
        }
    }

    @Override
    public String getStreamName(String event) {
        Object jsonEvent = Configuration.defaultConfiguration().jsonProvider().parse(event);
        return JsonPath.read(jsonEvent, streamNameField);
    }

    @Override
    public String getPartitionKey(String event) {
        Object jsonEvent = Configuration.defaultConfiguration().jsonProvider().parse(event);
        return JsonPath.read(jsonEvent, partitionKeyField);
    }

    @Override
    public String getData(String event) { return event; }
}
