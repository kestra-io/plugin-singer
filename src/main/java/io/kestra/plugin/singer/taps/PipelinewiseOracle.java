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
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "A Singer tap to fetch data from a Oracle database.",
    description = "Full documentation can be found [here](https://github.com/transferwise/pipelinewise-tap-oracle)"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "host: 127.0.0.1",
                "username: oracle",
                "password: oracle_passwd",
                "port: 63306",
                "sid: ORCL",
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
public class PipelinewiseOracle extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
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
    @PluginProperty
    private Integer port;

    @NotNull
    @Schema(
        title = "The database SID"
    )
    @PluginProperty(dynamic = true)
    private String sid;

    @Schema(
        title = "The schemas to filter"
    )
    @PluginProperty(dynamic = true)
    private String filterSchemas;

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
            .put("sid", runContext.render(this.sid));

        if (this.filterSchemas != null) {
            builder.put("filter_schemas", runContext.render(this.filterSchemas));
        }

        return builder.build();
    }

    @Override
    public List<String> pipPackages() {
        return Collections.singletonList("pipelinewise-tap-oracle");
    }

    @Override
    protected String command() {
        return "tap-oracle";
    }
}
