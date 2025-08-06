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
    title = "Fetch data from a MySQL database with a Singer tap.",
    description = "Full documentation can be found on the [GitHub Repo](https://transferwise.github.io/pipelinewise/connectors/taps/mysql.html)."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "host: 127.0.0.1",
                "username: root",
                "password: mysql_passwd",
                "port: 63306",
                "streamsConfigurations:",
                "  - stream: Category",
                "    replicationMethod: INCREMENTAL",
                "    replicationKeys: categoryId",
                "    selected: true",
                "  - propertiesPattern:",
                "      - description",
                "    selected: false",
            }
        )
    }
)
@Deprecated(forRemoval = true, since="0.24")
public class PipelinewiseMysql extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
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

    @Schema(deprecated = true,
        title = "If ssl is enabled."
    )
    @Builder.Default
    private final Property<Boolean> ssl = Property.ofValue(false);

    @Schema(deprecated = true,
        title = "The list of schemas to extract tables only from particular schemas and to improve data extraction performance."
    )
    private Property<List<String>> filterDbs;

    @Schema(deprecated = true,
        title = "Number of rows to export from MySQL in one batch."
    )
    @Builder.Default
    private final Property<Integer> exportBatchRows = Property.ofValue(50000);

    @Schema(deprecated = true,
        title = "List of SQL commands to run when a connection made. This allows to set session variables dynamically, like timeouts or charsets."
    )
    @Builder.Default
    private final Property<List<String>> sessionSqls = Property.ofValue(Arrays.asList(
        "SET @@session.time_zone=\"+0:00\"",
        "SET @@session.wait_timeout=28800",
        "SET @@session.net_read_timeout=3600",
        "SET @@session.innodb_lock_wait_timeout=3600"
    ));

    private static final String pipPackage = "pipelinewise-tap-mysql";
    private static final String comand = "tap-mysql";

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
            .put("ssl", runContext.render(this.ssl).as(Boolean.class).orElseThrow())
            .put("session_sqls", runContext.render(this.sessionSqls).asList(String.class))
            .put("export_batch_rows", runContext.render(this.exportBatchRows).as(Integer.class).orElseThrow());

        var filters = runContext.render(this.filterDbs).asList(String.class);
        if (!filters.isEmpty()) {
            builder.put("filter_dbs", String.join(",", filters));
        }

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.ofValue(Collections.singletonList("pipelinewise-tap-mysql"));
    }

    @Override
    protected Property<String> command() {
        return Property.ofValue("tap-mysql");
    }
}
