package io.kestra.plugin.singer.models;

import lombok.Builder;
import lombok.Value;

import java.util.Map;

@Value
@Builder
public class Metric {
    Type type;

    String metric;

    Double value;

    Map<String, Object> tags;

    public enum Type {
        counter,
        timer
    }
}
