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
    title = "Fetch data from an Oracle database with a Singer tap.",
    description = "Full documentation can be found on the [GitHub Repo](https://github.com/transferwise/pipelinewise-tap-oracle)."
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
@Deprecated(forRemoval = true, since="0.24")
public class PipelinewiseOracle extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
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
        title = "The database SID."
    )
    private Property<String> sid;

    @Schema(deprecated = true,
        title = "The schemas to filter."
    )
    private Property<String> filterSchemas;

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
            .put("sid", runContext.render(this.sid).as(String.class).orElseThrow());

        if (this.filterSchemas != null) {
            builder.put("filter_schemas", runContext.render(this.filterSchemas).as(String.class).orElseThrow());
        }

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.ofValue(Collections.singletonList("pipelinewise-tap-oracle"));
    }

    @Override
    protected Property<String> command() {
        return Property.ofValue("tap-oracle");
    }
}
