package io.kestra.plugin.singer.targets;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "A Singer target loads data into JSON files.",
    description = "Full documentation can be found [here](https://github.com/andyh1203/target-jsonl)"
)
public class Json extends AbstractPythonTarget implements RunnableTask<Json.Output> {
    private File destinationDirectory() {
        return new File(workingDirectory.toFile(), "destination");
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        File destination = destinationDirectory();
        destination.mkdir();

        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("do_timestamp_file", false)
            .put("destination_path", destination.getAbsolutePath());

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.of(Collections.singletonList("target-jsonl"));
    }

    @Override
    protected Property<String> command() {
        return Property.of("target-jsonl");
    }

    @Override
    public Json.Output run(RunContext runContext) throws Exception {
        AbstractPythonTarget.Output output = this.runTarget(runContext);

        return Output.builder()
            .stateKey(output.getStateKey())
            .uris(Arrays.stream(Objects.requireNonNull(destinationDirectory().listFiles()))
                .map(throwFunction(o -> {
                    List<String> name = new ArrayList<>(Arrays.asList(o.getName().split("\\.")));
                    name.remove(name.size() - 1);

                    return new AbstractMap.SimpleEntry<>(
                        String.join(".", name),
                        runContext.storage().putFile(o)
                    );
                }))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Key of the state in KV Store"
        )
        private final String stateKey;

        @Schema(
            title = "URI of the generated CSV.",
            description = "The key will be the name of the stream."
        )
        @PluginProperty(additionalProperties = URI.class)
        private final Map<String, URI> uris;
    }
}
