package io.kestra.plugin.singer.taps;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.singer.models.Feature;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

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
    title = "A Singer tap to fetch data from a Zoom account.",
    description = "Full documentation can be found [here](https://github.com/mashey/tap-zoom)"
)
public class Zoom extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @Schema(
        title = "Zoom JSON Web Token."
    )
    @PluginProperty(dynamic = true)
    private String jwt;

    @Schema(
        title = "Zoom client id."
    )
    @PluginProperty(dynamic = true)
    private String clientId;

    @Schema(
        title = "Zoom Client secret."
    )
    @PluginProperty(dynamic = true)
    private String clientSecret;

    @Schema(
        title = "Zoom refresh token."
    )
    @PluginProperty(dynamic = true)
    private String refreshToken;

    public List<Feature> features() {
        return Arrays.asList(
            Feature.CATALOG,
            Feature.DISCOVER
        );
    }

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();

        if (this.jwt != null) {
            builder.put("jwt", runContext.render(this.jwt));
        }

        if (this.clientId != null) {
            builder.put("client_id", runContext.render(this.clientId));
        }

        if (this.clientSecret != null) {
            builder.put("client_secret", runContext.render(this.clientSecret));
        }

        if (this.refreshToken != null) {
            builder.put("refresh_token", runContext.render(this.refreshToken));
        }

        return builder.build();
    }

    @Override
    public List<String> pipPackages() {
        return Collections.singletonList("git+https://github.com/mashey/tap-zoom.git");
    }

    @Override
    protected String command() {
        return "tap-zoom";
    }
}
