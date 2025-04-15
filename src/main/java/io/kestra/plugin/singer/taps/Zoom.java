package io.kestra.plugin.singer.taps;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
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
    title = "Fetch data from a Zoom account with a Singer tap.",
    description = "Full documentation can be found on the [GitHub Repo](https://github.com/mashey/tap-zoom)."
)
public class Zoom extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @Schema(
        title = "Zoom JSON Web Token."
    )
    private Property<String> jwt;

    @Schema(
        title = "Zoom client id."
    )
    private Property<String> clientId;

    @Schema(
        title = "Zoom Client secret."
    )
    private Property<String> clientSecret;

    @Schema(
        title = "Zoom refresh token."
    )
    private Property<String> refreshToken;

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
            builder.put("jwt", runContext.render(this.jwt).as(String.class).orElseThrow());
        }

        if (this.clientId != null) {
            builder.put("client_id", runContext.render(this.clientId).as(String.class).orElseThrow());
        }

        if (this.clientSecret != null) {
            builder.put("client_secret", runContext.render(this.clientSecret).as(String.class).orElseThrow());
        }

        if (this.refreshToken != null) {
            builder.put("refresh_token", runContext.render(this.refreshToken).as(String.class).orElseThrow());
        }

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.of(Collections.singletonList("git+https://github.com/mashey/tap-zoom.git"));
    }

    @Override
    protected Property<String> command() {
        return Property.of("tap-zoom");
    }
}
