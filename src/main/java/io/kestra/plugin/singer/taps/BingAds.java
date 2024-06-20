package io.kestra.plugin.singer.taps;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
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
    title = "A Singer tap to fetch data from a Bing ads.",
    description = "Full documentation can be found [here](https://github.com/singer-io/tap-bing-ads)"
)
public class BingAds extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @NotEmpty
    @Schema(
        title = "Your Developer Token for Bing Ads Application."
    )
    @PluginProperty(dynamic = true)
    private String developerToken;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Your OAuth Client ID."
    )
    @PluginProperty(dynamic = true)
    private String oauthClientId;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Your OAuth Client Secret."
    )
    @PluginProperty(dynamic = true)
    private String oauthClientSecret;

    @NotNull
    @NotEmpty
    @Schema(
        title = "The Refresh Token generated through the OAuth flow run using your OAuth Client and your Developer Token."
    )
    @PluginProperty(dynamic = true)
    private String refreshToken;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Your Customer ID."
    )
    @PluginProperty(dynamic = true)
    private String customerId;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Your accounts IDs."
    )
    @PluginProperty(dynamic = true)
    private List<String> accountIds;

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
            .put("developer_token", runContext.render(this.developerToken))
            .put("oauth_client_id", runContext.render(this.oauthClientId))
            .put("oauth_client_secret", runContext.render(this.oauthClientSecret))
            .put("refresh_token", runContext.render(this.refreshToken))
            .put("customer_id", runContext.render(this.customerId))
            .put("account_ids", String.join(",", runContext.render(this.accountIds)))
            .put("start_date", runContext.render(this.startDate.toString()));

        return builder.build();
    }

    @Override
    public List<String> pipPackages() {
        return Collections.singletonList("tap-bing-ads");
    }

    @Override
    protected String command() {
        return "tap-bing-ads";
    }
}
