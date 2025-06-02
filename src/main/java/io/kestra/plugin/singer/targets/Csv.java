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
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Load data into a CSV file with a Singer target.",
    description = "Full documentation can be found on the [GitHub Repo](https://github.com/adswerve/target-bigquery)."
)
public class Csv extends AbstractPythonTarget implements RunnableTask<Csv.Output> {
    @NotNull
    @NotEmpty
    @Schema(
        title = "A one-character string used to separate fields."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private final String delimiter = ",";

    @NotNull
    @NotEmpty
    @Schema(
        title = "A one-character string used to quote fields containing special characters, such as the delimiter or quotechar, or which contain new-line characters."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private final String quoteCharacters = "\"";

    private File destinationDirectory(RunContext runContext) {
        return new File(workingDirectory.toFile(), "destination");
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        File destination = destinationDirectory(runContext);
        destination.mkdir();

        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("delimiter", runContext.render(this.delimiter))
            .put("quotechar", runContext.render(this.quoteCharacters))
            .put("destination_path", destination.getAbsolutePath());

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.ofValue(Collections.singletonList("git+https://github.com/hotgluexyz/target-csv.git@0.3.6"));
    }

    @Override
    protected Property<String> command() {
        return Property.ofValue("target-csv");
    }

    @Override
    public Csv.Output run(RunContext runContext) throws Exception {
        AbstractPythonTarget.Output output = this.runTarget(runContext);

        return Output.builder()
            .stateKey(output.getStateKey())
            .uris(Arrays.stream(Objects.requireNonNull(destinationDirectory(runContext).listFiles()))
                .map(throwFunction(o -> {
                    List<String> name = new ArrayList<>(Arrays.asList(o.getName().split("-")));
                    name.remove(name.size() - 1);

                    return new AbstractMap.SimpleEntry<>(
                        String.join("-", name),
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
            title = "Key of the state file stored in the KV Store"
        )
        private final String stateKey;

        @Schema(
            title = "URIs of the generated CSV files",
            description = "The key corresponds to the name of the stream"
        )
        @PluginProperty(additionalProperties = URI.class)
        private final Map<String, URI> uris;
    }
}
