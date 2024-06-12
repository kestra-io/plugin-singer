package io.kestra.plugin.singer.taps;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.junit.annotations.KestraTest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@KestraTest
public abstract class TapsTest {
    public Map<StreamType, List<Map<String, Object>>> groupedByType(RunContext runContext, URI internalStorageUri) throws IOException {
        String rawOutput = new BufferedReader(new InputStreamReader(runContext.storage().getFile(internalStorageUri))).lines().collect(Collectors.joining("\n"));
        return Arrays.stream(rawOutput.split("\n"))
            .map(throwFunction(JacksonMapper::toMap))
            .collect(Collectors.groupingBy(object -> StreamType.value((String) object.getOrDefault("type", "UNKNOWN"))));
    }

    public enum StreamType {
        SCHEMA, STREAM, STATE, RECORD, UNKNOWN;

        static StreamType value(String type) {
            String typeUpper = type.toUpperCase();
            return Arrays.stream(values()).anyMatch(s -> s.name().equals(typeUpper)) ? StreamType.valueOf(type) : UNKNOWN;
        }
    }
}
