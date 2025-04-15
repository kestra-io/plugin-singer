package io.kestra.plugin.singer.taps;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.singer.models.Feature;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.LocalDate;
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
    title = "Fetch data from the HubSpot API with a Singer tap.",
    description = "Full documentation can be found on the [GitHub Repo](https://github.com/singer-io/tap-hubspot)."
)
public class HubSpot extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @Schema(
        title = "Hubspot redirect Uri"
    )
    @PluginProperty(dynamic = true)
    private Property<String> redirectUri = Property.of("https://api.hubspot.com/");

    @NotNull
    @Schema(
        title = "Hubspot client Id"
    )
    @PluginProperty(dynamic = true)
    private Property<String> clientId;

    @NotNull
    @Schema(
        title = "Hubspot client Secret"
    )
    @PluginProperty(dynamic = true)
    private Property<String> clientSecret;

    @NotNull
    @Schema(
        title = "Hubspot refresh Token"
    )
    @PluginProperty(dynamic = true)
    private Property<String> refreshToken;

    @NotNull
    @Schema(
        title = "Hubspot api Key (for development)",
        description = "As an alternative to OAuth 2.0 authentication during development, you may specify an API key to authenticate with the HubSpot API." +
            "This should be used only for low-volume development work, as the HubSpot API Usage Guidelines specify that integrations should use OAuth for authentication."
    )
    @PluginProperty(dynamic = true)
    private Property<String> apiKey;

    @NotNull
    @Schema(
        title = "Request timeout",
        description = "In seconds"
    )
    @PluginProperty(dynamic = true)
    private Property<Integer> requestTimeout;

    @NotNull
    @Schema(
        title = "Determines how much historical data will be extracted.",
        description = "Please be aware that the larger the time period and amount of data, the longer the initial extraction can be expected to take."
    )
    @PluginProperty(dynamic = true)
    private LocalDate startDate;

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
            .put("redirect_uri", runContext.render(this.clientId).as(String.class))
            .put("start_date", runContext.render(this.startDate.toString()));

        if (this.clientId != null) {
            builder.put("client_id", runContext.render(this.clientId).as(String.class));
        }

        if (this.clientSecret != null) {
            builder.put("client_secret", runContext.render(this.clientSecret).as(String.class));
        }

        if (this.refreshToken != null) {
            builder.put("refresh_token", runContext.render(this.refreshToken).as(String.class));
        }

        if (this.apiKey != null) {
            builder.put("hapikey", runContext.render(this.apiKey).as(String.class).orElseThrow());
        }

        if (this.requestTimeout != null) {
            builder.put("request_timeout", runContext.render(this.requestTimeout).as(Integer.class).orElseThrow());
        }

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.of(Collections.singletonList("tap-hubspot"));
    }

    @Override
    protected Property<String> command() {
        return Property.of("tap-hubspot");
    }
}
