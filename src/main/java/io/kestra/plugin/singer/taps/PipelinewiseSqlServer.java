package io.kestra.plugin.singer.taps;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(deprecated = true,
    title = "Fetch data from a Microsoft SQL Server database with a Singer tap.",
    description = "Full documentation can be found on the [GitHub Repo](https://github.com/BuzzCutNorman/tap-mssql/blob/main/README.md)."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "host: 127.0.0.1",
                "username: SA",
                "password: sqlserver_passwd",
                "port: 57037",
                "filterDbs: dbo",
                "streamsConfigurations:",
                "  - stream: Categories",
                "    replicationMethod: INCREMENTAL",
                "    replicationKeys: CategoryID",
                "    selected: true",
                "  - propertiesPattern:",
                "      - Description",
                "    selected: false",
            }
        )
    }
)
@Deprecated(forRemoval = true, since="0.24")
public class PipelinewiseSqlServer extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotEmpty
    @Schema(deprecated = true,
        title = "The database hostname."
    )
    @PluginProperty(dynamic = true)
    private String host;

    @NotEmpty
    @Schema(deprecated = true,
        title = "The database name."
    )
    @PluginProperty(dynamic = true)
    private String database;

    @NotNull
    @Schema(deprecated = true,
        title = "The database port."
    )
    @PluginProperty
    private Property<Integer> port;

    @NotEmpty
    @Schema(deprecated = true,
        title = "The database user."
    )
    @PluginProperty(dynamic = true)
    private String username;

    @NotEmpty
    @Schema(deprecated = true,
        title = "The database user's password."
    )
    @PluginProperty(dynamic = true)
    private String password;

    @Schema(deprecated = true,
        title = "The list of schemas to extract tables only from particular schemas and to improve data extraction performance."
    )
    private Property<List<String>> filterDbs;

    @NotNull
    @Schema(deprecated = true,
        title = "Emit Date datatypes as-is without converting them to datetime."
    )
    @Builder.Default
    private Property<Boolean> useDateDatatype = Property.ofValue(true);

    @Schema(deprecated = true,
        title = "TDS version to use when communicating with SQL Server (default is 7.3)."
    )
    private Property<String> tdsVersion;

    @Schema(deprecated = true,
        title = "The characterset for the database / source system. The default is utf8, however older databases might use a charactersets like cp1252 for the encoding."
    )
    private Property<String> characterSet;

    @Schema(deprecated = true,
        title = "Emit all numeric values as strings and treat floats as string data types for the target (default false).",
        description = "When true, the resulting SCHEMA message will contain an attribute in additionalProperties containing the scale and precision of the discovered property."
    )
    private Property<Boolean> useSingerDecimal;

    @Schema(deprecated = true,
        title = "A numeric setting adjusting the internal buffersize for the tap (default 10000).",
        description = "The common query tuning scenario is for SELECT statements that return a large number of rows over a slow network. Increasing arraysize can improve performance by reducing the number of round-trips to the database. However increasing this value increases the amount of memory required."
    )
    private Property<Integer> cursorArraySize;

    public List<Feature> features() {
        return Arrays.asList(
            Feature.CATALOG,
            Feature.DISCOVER,
            Feature.STATE,
            Feature.PROPERTIES
        );
    }

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("host", runContext.render(this.host))
            .put("port", runContext.render(this.port).as(Integer.class).orElseThrow())
            .put("user", runContext.render(this.username))
            .put("password", runContext.render(this.password))
            .put("database", runContext.render(this.database))
            .put("use_date_datatype", runContext.render(this.useDateDatatype).as(Boolean.class).orElseThrow());

        var filters = runContext.render(this.filterDbs).asList(String.class);
        if (!filters.isEmpty()) {
            builder.put("filter_dbs", String.join(",", filters));
        }

        if (this.tdsVersion != null) {
            builder.put("tds_version", runContext.render(this.tdsVersion).as(String.class).orElseThrow());
        }

        if (this.characterSet != null) {
            builder.put("characterset", runContext.render(this.characterSet).as(String.class).orElseThrow());
        }

        if (this.useSingerDecimal != null) {
            builder.put("use_singer_decimal", runContext.render(this.useSingerDecimal).as(Boolean.class).orElseThrow());
        }

        if (this.cursorArraySize != null) {
            builder.put("cursor_array_size", runContext.render(this.cursorArraySize).as(Integer.class).orElseThrow());
        }

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.ofValue(Collections.singletonList("tap-mssql==2.3.1"));
    }

    @Override
    protected Property<String> command() {
        return Property.ofValue("tap-mssql");
    }
}
