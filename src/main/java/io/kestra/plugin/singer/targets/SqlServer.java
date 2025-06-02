package io.kestra.plugin.singer.targets;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.StringUtils;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Load data into a Microsoft SQL Server database with a Singer target.",
    description = "Full documentation can be found on the [GitHub Repo](https://github.com/storebrand/target-mssql/blob/main/README.md)."
)
public class SqlServer extends AbstractPythonTarget implements RunnableTask<AbstractPythonTarget.Output> {
    @NotEmpty
    @Schema(
        title = "The database hostname."
    )
    @PluginProperty(dynamic = true)
    private String host;

    @NotEmpty
    @Schema(
        title = "The database name."
    )
    @PluginProperty(dynamic = true)
    private String database;

    @NotNull
    @Schema(
        title = "The database port."
    )
    private Property<Integer> port;

    @NotEmpty
    @Schema(
        title = "The database user."
    )
    @PluginProperty(dynamic = true)
    private String username;

    @NotEmpty
    @Schema(
        title = "The database user's password."
    )
    @PluginProperty(dynamic = true)
    private String password;

    @Schema(
        title = "Default target schema to write to."
    )
    private Property<String> defaultTargetSchema;

    @Schema(
        title = "Prefix to add to table name. Useful if retrieving data from multiple taps for easier filtering."
    )
    private Property<String> tablePrefix;

    @Schema(
        title = "Use float data type for numbers (otherwise number type is used)."
    )
    private Property<Boolean> preferFloatOverNumeric;

    @Schema(
        title = "Config object for stream maps capability."
    )
    private Property<String> streamMaps;

    @Schema(
        title = "User-defined config values to be used within map expressions."
    )
    private Property<String> streamMapConfig;

    @Schema(
        title = "Enable schema flattening and automatically expand nested properties"
    )
    private Property<Boolean> flatteningEnabled;

    @Schema(
        title = "The max depth to flatten schemas."
    )
    private Property<Integer> flatteningMaxDepth;

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("username", runContext.render(this.username))
            .put("password", runContext.render(this.password))
            .put("host", runContext.render(this.host))
            .put("port", String.valueOf(runContext.render(this.port).as(Integer.class).orElseThrow()))
            .put("database", runContext.render(this.database));

        if (this.defaultTargetSchema != null) {
            builder.put("default_target_schema", runContext.render(this.defaultTargetSchema).as(String.class).orElseThrow());
        }

        if (this.tablePrefix != null) {
            builder.put("table_prefix", runContext.render(this.tablePrefix).as(String.class).orElseThrow());
        }

        if (this.preferFloatOverNumeric != null) {
            builder.put("prefer_float_over_numeric", StringUtils.capitalize(runContext.render(this.preferFloatOverNumeric).as(Boolean.class).orElseThrow().toString()));
        }

        if (this.streamMaps != null) {
            builder.put("stream_maps", runContext.render(this.streamMaps).as(String.class).orElseThrow());
        }

        if (this.streamMapConfig != null) {
            builder.put("stream_map_config", runContext.render(this.streamMapConfig).as(String.class).orElseThrow());
        }

        if (this.flatteningEnabled != null) {
            builder.put("flattening_enabled", StringUtils.capitalize(runContext.render(this.flatteningEnabled).as(Boolean.class).orElseThrow().toString()));
        }

        if (this.flatteningMaxDepth != null) {
            builder.put("flattening_max_depth", runContext.render(this.flatteningMaxDepth).as(Integer.class).orElseThrow());
        }

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.ofValue(Collections.singletonList("target-mssql"));
    }

    @Override
    protected Property<String> command() {
        return Property.ofValue("target-mssql");
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        return super.runTarget(runContext);
    }
}
