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
    title = "A Singer tap to fetch data from a google search console.",
    description = "Full documentation can be found [here](https://github.com/singer-io/tap-google-search-console)"
)
public class GoogleSearchConsole extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @NotEmpty
    @Schema(
        title = "Your Google OAuth Client ID."
    )
    @PluginProperty(dynamic = true)
    private String clientId;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Your Google OAuth Client Secret."
    )
    @PluginProperty(dynamic = true)
    private String clientSecret;

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
        title = "website URL properties",
        description = "Do not include the domain-level property in the list"
    )
    @PluginProperty(dynamic = true)
    private List<String> siteUrls;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Determines how much historical data will be extracted.",
        description = "Please be aware that the larger the time period and amount of data, the longer the initial extraction can be expected to take."
    )
    @PluginProperty(dynamic = true)
    private LocalDate startDate;

    @Schema(
        title = "User Agent for your OAuth Client."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private final String userAgent = "tap-google-search-console via Kestra";

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
            .put("client_id", runContext.render(this.clientId))
            .put("client_secret", runContext.render(this.clientSecret))
            .put("refresh_token", runContext.render(this.refreshToken))
            .put("site_urls", String.join(", ", runContext.render(this.siteUrls)))
            .put("start_date", runContext.render(this.startDate.toString()));

        if (this.userAgent != null) {
            builder.put("user_agent", runContext.render(this.userAgent));
        }

        return builder.build();
    }

    @Override
    public List<String> pipPackages() {
        return Collections.singletonList("tap-google-search-console");
    }

    @Override
    protected String command() {
        return "tap-google-search-console";
    }
}
