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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(deprecated = true,
    title = "Fetch data from a MongoDB database with a Singer tap.",
    description = "Full documentation can be found on the [GitHub Repo](https://transferwise.github.io/pipelinewise/connectors/taps/mongodb.html)."
)
@Deprecated(forRemoval = true, since="0.24")
public class PipelinewiseMongoDb extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @NotEmpty
    @Schema(deprecated = true,
        title = "The database hostname."
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

    @NotNull
    @Schema(deprecated = true,
        title = "The database port."
    )
    private Property<Integer> port;

    @NotNull
    @Schema(deprecated = true,
        title = "The database name."
    )
    private Property<String> database;

    @NotNull
    @Schema(deprecated = true,
        title = "The database name to authenticate on."
    )
    private Property<String> authDatabase;

    @Schema(deprecated = true,
        title = "If ssl is enabled."
    )
    @Builder.Default
    private final Property<Boolean> ssl = Property.ofValue(false);

    @Schema(deprecated = true,
        title = "Default SSL verify mode."
    )
    @Builder.Default
    private final Property<Boolean> sslVerify = Property.ofValue(true);

    @Schema(deprecated = true,
        title = "The name of replica set."
    )
    private Property<String> replicaSet;

    @Schema(deprecated = true,
        title = "Forces the stream names to take the form `<database_name>_<collection_name>` instead of `<collection_name>`."
    )
    @Builder.Default
    private final Property<Boolean> includeSchemaInStream = Property.ofValue(false);

    @Schema(deprecated = true,
        title = "The size of the buffer that holds detected update operations in memory.",
        description = "For LOG_BASED only, the buffer is flushed once the size is reached."
    )
    @Builder.Default
    private final Property<Integer> updateBufferSize = Property.ofValue(1);

    @Schema(deprecated = true,
        title = "The maximum amount of time in milliseconds waits for new data changes before exiting.",
        description = "For LOG_BASED only."
    )
    @Builder.Default
    private final Property<Integer> awaitTimeMs = Property.ofValue(1000);

    public List<Feature> features() {
        return Arrays.asList(
            Feature.CATALOG,
            Feature.DISCOVER,
            Feature.STATE
        );
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.ofValue(Collections.singletonList("pipelinewise-tap-mongodb"));
    }

    @Override
    protected Property<String> command() {
        return Property.ofValue("tap-mongodb");
    }

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("user", runContext.render(this.username))
            .put("password", runContext.render(this.password).as(String.class).orElse(null))
            .put("host", runContext.render(this.host))
            .put("port", runContext.render(this.port).as(Integer.class).orElseThrow())
            .put("ssl", runContext.render(this.ssl).as(Boolean.class).orElseThrow().toString())
            .put("verify_mode", runContext.render(this.sslVerify).as(Boolean.class).orElseThrow().toString())
            .put("database", runContext.render(this.database).as(String.class).orElseThrow())
            .put("auth_database", runContext.render(this.authDatabase).as(String.class).orElseThrow())

            .put("include_schema_in_destination_stream_name", runContext.render(this.includeSchemaInStream).as(Boolean.class).orElseThrow())
            .put("update_buffer_size", runContext.render(this.updateBufferSize).as(Integer.class).orElseThrow())
            .put("await_time_ms", runContext.render(this.awaitTimeMs).as(Integer.class).orElseThrow());

        if (this.replicaSet != null) {
            builder.put("replica_set", runContext.render(this.replicaSet).as(String.class).orElseThrow());
        }

        return builder.build();
    }
}
