package io.kestra.plugin.singer.taps;

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

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(deprecated = true,
    title = "Fetch data from a PostgreSQL database with a Singer tap.",
    description = "Full documentation can be found on the [GitHub Repo](https://github.com/transferwise/pipelinewise-tap-postgres)."
)
@Deprecated(forRemoval = true, since="0.24")
public class PipelinewisePostgres extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @NotEmpty
    @Schema(deprecated = true,
        title = "The database hostname"
    )
    @PluginProperty(dynamic = true)
    private String host;

    @NotNull
    @NotEmpty
    @Schema(deprecated = true,
        title = "The database user."
    )
    @PluginProperty(dynamic = true)
    private String username;

    @Schema(deprecated = true,
        title = "The database user's password."
    )
    private Property<String> password;

    @Schema(deprecated = true,
        title = "The database name."
    )
    private Property<String> dbName;

    @NotNull
    @Schema(deprecated = true,
        title = "The database port."
    )
    private Property<Integer> port;

    @Schema(deprecated = true,
        title = "If ssl is enabled."
    )
    @Builder.Default
    private final Property<Boolean> ssl = Property.ofValue(false);

    @Schema(deprecated = true,
        title = "Stop running the tap when no data received from wal after certain number of seconds."
    )
    @Builder.Default
    private final Property<Integer> logicalPollSeconds = Property.ofValue(10800);

    @Schema(deprecated = true,
        title = "Stop running the tap if the newly received lsn is after the max lsn that was detected when the tap started."
    )
    @Builder.Default
    private final Property<Boolean> breakAtEndLsn = Property.ofValue(true);

    @Schema(deprecated = true,
        title = "Stop running the tap after certain number of seconds."
    )
    @Builder.Default
    private final Property<Integer> maxRunSeconds = Property.ofValue(43200);

    @Schema(deprecated = true,
        title = "If set to \"true\" then add _sdc_lsn property to the singer messages to debug postgres LSN position in the WAL stream."
    )
    @Builder.Default
    private final Property<Boolean> debugLsn = Property.ofValue(false);

    @Schema(deprecated = true,
        title = "The list of schemas to extract tables only from particular schemas and to improve data extraction performance"
    )
    private Property<List<String>> filterSchemas;

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
            .put("user", runContext.render(this.username))
            .put("password", runContext.render(this.password).as(String.class).orElse(null))
            .put("host", runContext.render(this.host))
            .put("port", runContext.render(this.port).as(Integer.class).orElseThrow())
            .put("dbname", runContext.render(this.dbName).as(String.class).orElseThrow())
            .put("ssl", runContext.render(this.ssl).as(Boolean.class).orElseThrow())
            .put("logical_poll_seconds", runContext.render(this.logicalPollSeconds).as(Integer.class).orElseThrow())
            .put("break_at_end_lsn", runContext.render(this.breakAtEndLsn).as(Boolean.class).orElseThrow())
            .put("max_run_seconds", runContext.render(this.maxRunSeconds).as(Integer.class).orElseThrow())
            .put("debug_lsn", runContext.render(this.debugLsn).as(Boolean.class).orElseThrow());

        var filters = runContext.render(this.filterSchemas).asList(String.class);
        if (!filters.isEmpty()) {
            builder.put("filter_dbs", String.join(",", filters));
        }

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.ofValue(List.of("pipelinewise-tap-postgres"));
    }

    @Override
    protected Property<String> command() {
        return Property.ofValue("tap-postgres");
    }
}
