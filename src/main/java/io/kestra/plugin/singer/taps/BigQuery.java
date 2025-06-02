package io.kestra.plugin.singer.taps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.singer.models.Feature;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Fetch data from BigQuery with a Singer tap.",
    description = "Full documentation can be found on the [GitHub Repo](https://github.com/anelendata/tap-bigquery)."
)
public class BigQuery extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @Schema(
        title = "The JSON service account key as string."
    )
    protected Property<String> serviceAccount;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Array holding objects describing streams (tables) to extract, with `name`, `table`, `columns`, `datetime_key`, and `filters` keys."
    )
    @PluginProperty
    protected List<Stream> streams;

    @NotNull
    @Schema(
        title = "Limits the number of records returned in each stream, applied as a limit in the query."
    )
    private Property<Integer> limit;

    @NotNull
    @Schema(
        title = "When replicating incrementally, disable to only select records whose `datetime_key` is greater than the maximum value replicated in the last run, by excluding records whose timestamps match exactly.",
        description = "This could cause records to be missed that were created after the last run finished, but during the same second and with the same timestamp."
    )
    @Builder.Default
    private Property<Boolean> startAlwaysInclusive = Property.ofValue(true);

    @NotNull
    @Schema(
        title = "Determines how much historical data will be extracted.",
        description = "Please be aware that the larger the time period and amount of data, the longer the initial extraction can be expected to take."
    )
    @PluginProperty(dynamic = true)
    private Instant startDateTime;

    @Schema(
        title = "Date up to when historical data will be extracted."
    )
    @PluginProperty(dynamic = true)
    private Instant endDateTime;

    public List<Feature> features() {
        return Arrays.asList(
            Feature.CATALOG,
            Feature.DISCOVER,
            Feature.STATE
        );
    }

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("streams", this.streams)
            .put("start_always_inclusive", runContext.render(this.startAlwaysInclusive).as(Boolean.class).orElseThrow());

        if (this.startDateTime != null) {
            builder.put("start_datetime", runContext.render(this.startDateTime.toString()));
        }

        if (this.endDateTime != null) {
            builder.put("end_datetime", runContext.render(this.endDateTime.toString()));
        }

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.ofValue(List.of("git+https://github.com/kestra-io/tap-bigquery.git@fix"));
    }

    @Override
    protected Property<String> command() {
        return Property.ofValue("tap-bigquery");
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    protected Map<String, String> environmentVariables(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        HashMap<String, String> env = new HashMap<>(super.environmentVariables(runContext));

        if (this.serviceAccount != null) {
            this.writeSingerFiles("google-credentials.json", runContext.render(this.serviceAccount).as(String.class).orElseThrow());
            env.put("GOOGLE_APPLICATION_CREDENTIALS", workingDirectory.toAbsolutePath() + "/google-credentials.json");
        }

        env.put("PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION", "python");

        return env;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Builder
    public static class Stream {
        String name;
        String table;
        List<String> columns;

        @JsonProperty("datetime_key")
        String datetimeKey;

        @Schema(
            title = "these are parsed in `WHERE` clause",
            description = "filters are optional but we strongly recommend using this over a large partitioned table to control the cost."
        )
        List<String> filters;
    }
}
