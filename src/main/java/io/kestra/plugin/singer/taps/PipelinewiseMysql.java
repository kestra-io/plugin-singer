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
    title = "A Singer tap to fetch data from a MySQL database.",
    description = "Full documentation can be found [here](https://transferwise.github.io/pipelinewise/connectors/taps/mysql.html)"
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
                " - stream: Category",
                "   replicationMethod: INCREMENTAL",
                "   replicationKeys: categoryId",
                "   selected: true",
                "- propertiesPattern:",
                "   - description",
                "  selected: false",
            }
        )
    }
)
public class PipelinewiseMysql extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
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

    @Schema(
        title = "If ssl is enabled"
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private final Boolean ssl = false;

    @Schema(
        title = "The list of schemas to extract tables only from particular schemas and to improve data extraction performance"
    )
    @PluginProperty(dynamic = true)
    private List<String> filterDbs;

    @Schema(
        title = "Number of rows to export from MySQL in one batch."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private final Integer exportBatchRows = 50000;

    @Schema(
        title = "List of SQL commands to run when a connection made. This allows to set session variables dynamically, like timeouts or charsets."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private final List<String> sessionSqls = Arrays.asList(
        "SET @@session.time_zone=\"+0:00\"",
        "SET @@session.wait_timeout=28800",
        "SET @@session.net_read_timeout=3600",
        "SET @@session.innodb_lock_wait_timeout=3600"
    );

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
            .put("password", runContext.render(this.password))
            .put("host", runContext.render(this.host))
            .put("port", this.port)
            .put("ssl", this.ssl)
            .put("session_sqls", runContext.render(this.sessionSqls))
            .put("export_batch_rows", this.exportBatchRows);

        if (this.filterDbs != null) {
            builder.put("filter_dbs", String.join(",", runContext.render(this.filterDbs)));
        }

        return builder.build();
    }

    @Override
    public List<String> pipPackages() {
        return Collections.singletonList("pipelinewise-tap-mysql");
    }

    @Override
    protected String command() {
        return "tap-mysql";
    }
}
