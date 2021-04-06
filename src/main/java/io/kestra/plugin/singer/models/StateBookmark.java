package io.kestra.plugin.singer.models;

import lombok.Builder;
import lombok.Value;

import java.util.Map;

@Value
@Builder
public class StateBookmark {
    Map<String, Object> bookmarks;
}
