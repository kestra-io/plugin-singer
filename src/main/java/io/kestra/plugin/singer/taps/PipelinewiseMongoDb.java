package io.kestra.plugin.singer.taps;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.singer.models.Feature;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "A Singer tap to fetch data from a MongoDb database.",
    description = "Full documentation can be found [here](https://transferwise.github.io/pipelinewise/connectors/taps/mongodb.html)"
)
public class PipelinewiseMongoDb extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @NotEmpty
    @Schema(
        title = "The database hostname"
    )
    @PluginProperty(dynamic = true)
    private String host;

    @NotNull
    @NotEmpty
    @Schema(
        title = "The database user"
    )
    @PluginProperty(dynamic = true)
    private String username;

    @Schema(
        title = "The database user's password"
    )
    @PluginProperty(dynamic = true)
    private String password;

    @NotNull
    @Schema(
        title = "The database port"
    )
    @PluginProperty(dynamic = false)
    private Integer port;

    @NotNull
    @Schema(
        title = "The database name."
    )
    @PluginProperty(dynamic = true)
    private String database;

    @NotNull
    @Schema(
        title = "The database name to authenticate on."
    )
    @PluginProperty(dynamic = true)
    private String authDatabase;

    @Schema(
        title = "If ssl is enabled."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private final Boolean ssl = false;

    @Schema(
        title = "Default SSL verify mode."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private final Boolean sslVerify = true;

    @Schema(
        title = "The name of replica set."
    )
    @PluginProperty(dynamic = true)
    private String replicaSet;

    @Schema(
        title = "Forces the stream names to take the form `<database_name>_<collection_name>` instead of `<collection_name>`."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private final Boolean includeSchemaInStream = false;

    @Schema(
        title = "The size of the buffer that holds detected update operations in memory.",
        description = "For LOG_BASED only, the buffer is flushed once the size is reached"
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private final Integer updateBufferSize = 1;

    @Schema(
        title = "The maximum amount of time in milliseconds waits for new data changes before exiting..",
        description = "For LOG_BASED only."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private final Integer awaitTimeMs = 1000;

    public List<Feature> features() {
        return Arrays.asList(
            Feature.CATALOG,
            Feature.DISCOVER,
            Feature.STATE
        );
    }

    @Override
    public List<String> pipPackages() {
        return Collections.singletonList("pipelinewise-tap-mongodb");
    }

    @Override
    protected String command() {
        return "tap-mongodb";
    }

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("user", runContext.render(this.username))
            .put("password", runContext.render(this.password))
            .put("host", runContext.render(this.host))
            .put("port", this.port)
            .put("ssl", this.ssl)
            .put("verify_mode", this.sslVerify)
            .put("database", runContext.render(this.database))
            .put("auth_database", runContext.render(this.authDatabase))

            .put("include_schema_in_destination_stream_name", this.includeSchemaInStream)
            .put("update_buffer_size", this.updateBufferSize)
            .put("await_time_ms", this.awaitTimeMs);

        if (this.replicaSet != null) {
            builder.put("replica_set", runContext.render(this.replicaSet));
        }

        return builder.build();
    }
}
