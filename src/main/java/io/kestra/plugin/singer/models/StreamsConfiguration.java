package io.kestra.plugin.singer.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

import java.util.List;
import javax.annotation.RegEx;

@Value
@Builder
public class StreamsConfiguration {
    @Nullable
    String stream;

    DiscoverMetadata.ReplicationMethod replicationMethod;

    String replicationKeys;

    @RegEx
    List<String> propertiesPattern;

    @Builder.Default
    @JsonInclude
    Boolean selected = true;
}
