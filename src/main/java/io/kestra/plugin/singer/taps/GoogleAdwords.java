package io.kestra.plugin.singer.taps;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.singer.models.Feature;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.LocalDate;
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
    title = "A Singer tap to fetch data from Google Adwords.",
    description = "Full documentation can be found [here](https://gitlab.com/meltano/tap-adwords)"
)
public class GoogleAdwords extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @NotEmpty
    @Schema(
        title = "Your developer token for Google AdWord application."
    )
    @PluginProperty(dynamic = true)
    private String developerToken;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Your Google OAuth Client ID."
    )
    @PluginProperty(dynamic = true)
    private String oauthClientId;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Your Google OAuth Client Secret."
    )
    @PluginProperty(dynamic = true)
    private String oauthClientSecret;

    @NotNull
    @NotEmpty
    @Schema(
        title = "The Refresh Token generated through the OAuth flow run using your OAuth Client and your developer token."
    )
    @PluginProperty(dynamic = true)
    private String refreshToken;

    @NotNull
    @NotEmpty
    @Schema(
        title = "A list of Ad Account IDs to replicate data from."
    )
    @PluginProperty(dynamic = true)
    private List<String> customerIds;

    @NotNull
    @Schema(
        title = "Determines how much historical data will be extracted.",
        description = "Please be aware that the larger the time period and amount of data, the longer the initial extraction can be expected to take."
    )
    @PluginProperty(dynamic = true)
    private LocalDate startDate;

    @Schema(
        title = "Date up to when historical data will be extracted."
    )
    @PluginProperty(dynamic = true)
    private LocalDate endDate;

    @Schema(
        title = "How many Days before the Start Date to fetch data for Performance Reports."
    )
    @PluginProperty
    @Builder.Default
    private final Integer conversionWindowDays = 0;

    @Schema(
        title = "Primary Keys for the selected Entities (Streams)."
    )
    @PluginProperty
    private Map<String, List<String>> primaryKeys;

    @Schema(
        title = "User Agent for your OAuth Client."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private final String userAgent = "tap-adwords via Kestra";

    public List<Feature> features() {
        return Arrays.asList(
            Feature.PROPERTIES,
            Feature.DISCOVER,
            Feature.STATE
        );
    }

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("developer_token", runContext.render(this.developerToken))
            .put("oauth_client_id", runContext.render(this.oauthClientId))
            .put("oauth_client_secret", runContext.render(this.oauthClientSecret))
            .put("refresh_token", runContext.render(this.refreshToken))
            .put("customer_ids", String.join(",", runContext.render(this.customerIds)))
            .put("start_date", runContext.render(this.startDate.toString()))
            .put("conversion_window_days", this.conversionWindowDays);

        if (this.endDate != null) {
            builder.put("end_date", runContext.render(this.endDate.toString()));
        }

        if (this.userAgent != null) {
            builder.put("user_agent", runContext.render(this.userAgent));
        }

        if (this.primaryKeys != null) {
            builder.put("primary_keys", this.primaryKeys);
        }

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.of(Collections.singletonList("git+https://gitlab.com/meltano/tap-adwords.git"));
    }

    @Override
    protected Property<String> command() {
        return Property.of("tap-adwords");
    }
}
