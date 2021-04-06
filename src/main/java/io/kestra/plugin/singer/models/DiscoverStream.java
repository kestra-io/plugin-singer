package io.kestra.plugin.singer.models;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Value
public class DiscoverStream {
    @JsonProperty("tap_stream_id")
    String tapStreamId;

    String stream;

    Map<String, Object> schema;

    @JsonProperty("table_name")
    String tableName;

    @JsonProperty("key_properties")
    List<String> keyProperties;

    @With
    List<Metadata> metadata;

    @Getter(AccessLevel.NONE)
    Map<String, Object> extra = new HashMap<>();

    @JsonAnyGetter
    public Map<String, Object> getExtraFields(){
        return extra;
    }

    @JsonAnySetter
    public void setExtra(String name, Object value) {
        extra.put(name, value);
    }

    @Value
    @Builder
    public static class Metadata {
        @With
        DiscoverMetadata metadata;

        @Builder.Default
        List<String> breadcrumb = new ArrayList<>();

        public String breadcrumb() {
            ArrayList<String> breadcrumbs = new ArrayList<>(breadcrumb);

            breadcrumbs.removeIf(s -> s.equals("properties"));

            return String.join(".", breadcrumbs);
        }
    }
}
