package com.monetate.koupler.format;

/**
 * Created by brianoneill on 4/29/16.
 */
public interface Format {

    String getStreamName(String event);

    String getPartitionKey(String event);

    String getData(String event);

}
