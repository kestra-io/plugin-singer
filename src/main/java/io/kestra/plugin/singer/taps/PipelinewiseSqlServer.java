package io.kestra.plugin.singer.taps;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.singer.models.Feature;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "A Singer tap to fetch data from a Microsoft SQL Server database.",
    description = "Full documentation can be found [here](https://github.com/BuzzCutNorman/tap-mssql/blob/main/README.md)"
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
public class PipelinewiseSqlServer extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
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
        title = "The database name"
    )
    @PluginProperty(dynamic = true)
    private String database;

    @NotNull
    @NotEmpty
    @Schema(
        title = "The database port"
    )
    @PluginProperty
    private Integer port;

    @NotNull
    @NotEmpty
    @Schema(
        title = "The database user"
    )
    @PluginProperty(dynamic = true)
    private String username;

    @NotNull
    @NotEmpty
    @Schema(
        title = "The database user's password"
    )
    @PluginProperty(dynamic = true)
    private String password;

    @Schema(
        title = "Schema to extract tables only from particular schemas and to improve data extraction performance"
    )
    @PluginProperty(dynamic = true)
    private String filterDbs;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Emit Date datatypes as-is without converting them to datetime"
    )
    @PluginProperty
    @Builder.Default
    private Boolean useDateDatatype = true;

    @Schema(
        title = "TDS verison to use when communicating with SQL Server (default is 7.3)"
    )
    @PluginProperty(dynamic = true)
    private String tdsVersion;

    @Schema(
        title = "The characterset for the database / source system. The default is utf8, however older databases might use a charactersets like cp1252 for the encoding"
    )
    @PluginProperty(dynamic = true)
    private String characterSet;

    @Schema(
        title = "Emit all numeric values as strings and treat floats as string data types for the target (default false)",
        description = "When true, the resulting SCHEMA message will contain an attribute in additionalProperties containing the scale and precision of the discovered property."
    )
    @PluginProperty
    private Boolean useSingerDecimal;

    @Schema(
        title = "A numeric setting adjusting the internal buffersize for the tap (default 10000)",
        description = "The common query tuning scenario is for SELECT statements that return a large number of rows over a slow network. Increasing arraysize can improve performance by reducing the number of round-trips to the database. However increasing this value increases the amount of memory required."
    )
    @PluginProperty
    private Integer cursorArraySize;

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
            .put("port", this.port)
            .put("user", runContext.render(this.username))
            .put("password", runContext.render(this.password))
            .put("database", runContext.render(this.database))
            .put("use_date_datatype", this.useDateDatatype);

        if (this.filterDbs != null) {
            builder.put("filter_dbs", this.filterDbs);
        }

        if (this.tdsVersion != null) {
            builder.put("tds_version", this.tdsVersion);
        }

        if (this.characterSet != null) {
            builder.put("characterset", this.characterSet);
        }

        if (this.useSingerDecimal != null) {
            builder.put("use_singer_decimal", this.useSingerDecimal);
        }

        if (this.cursorArraySize != null) {
            builder.put("cursor_array_size", this.cursorArraySize);
        }

        return builder.build();
    }

    @Override
    public List<String> pipPackages() {
        return Collections.singletonList("tap-mssql");
    }

    @Override
    protected String command() {
        return "tap-mssql";
    }
}
