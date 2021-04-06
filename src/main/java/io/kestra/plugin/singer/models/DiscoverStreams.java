package io.kestra.plugin.singer.models;

import lombok.Value;

import java.util.List;

@Value
public class DiscoverStreams {
    List<DiscoverStream> streams;
}
