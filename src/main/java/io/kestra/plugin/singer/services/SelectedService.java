package io.kestra.plugin.singer.services;

import io.kestra.plugin.singer.models.DiscoverMetadata;
import io.kestra.plugin.singer.models.DiscoverStream;
import io.kestra.plugin.singer.models.DiscoverStreams;
import io.kestra.plugin.singer.models.StreamsConfiguration;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SelectedService {
    public static DiscoverStreams fill(DiscoverStreams discoverStreams, List<StreamsConfiguration> streamConfigs) {
        Objects.requireNonNull(streamConfigs, "illegal null 'streamConfigs'");

        final List<StreamsConfiguration> finalConfigs = streamConfigs;

        return new DiscoverStreams(discoverStreams
            .getStreams()
            .stream()
            .map(discoverStream -> {
                for (StreamsConfiguration config : finalConfigs) {
                    if (config.getStream() == null || config.getStream().equals(discoverStream.getStream())) {
                        discoverStream = selectStream(discoverStream, config);
                    }
                }

                return discoverStream;
            })
            .collect(Collectors.toList())
        );
    }

    private static boolean matchProperties(DiscoverStream.Metadata metadata, List<String> propertiesPattern) {
        if (propertiesPattern == null || propertiesPattern.size() == 0) {
            return true;
        }

        return propertiesPattern
            .stream()
            .anyMatch(s -> metadata.breadcrumb().matches(s));
    }

    private static DiscoverStream selectStream(DiscoverStream discoverStream, StreamsConfiguration streamsConfiguration) {
        return discoverStream.withMetadata(
            discoverStream
                .getMetadata()
                .stream()
                .map(metadata -> {
                    if (!matchProperties(metadata, streamsConfiguration.getPropertiesPattern())) {
                        return metadata;
                    }

                    DiscoverMetadata converted = metadata.getMetadata()
                        .withSelected(streamsConfiguration.getSelected());

                    if (metadata.getBreadcrumb().size() == 0) {
                        converted = converted
                            .withReplicationMethod(streamsConfiguration.getReplicationMethod())
                            .withReplicationKey(streamsConfiguration.getReplicationKeys());
                    }

                    return metadata
                        .withMetadata(converted);
                })
                .collect(Collectors.toList())
        );
    }
}
