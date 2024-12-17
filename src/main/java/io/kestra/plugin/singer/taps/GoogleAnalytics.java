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

import java.io.IOException;
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
    title = "A Singer tap to fetch data from from the Google Analytics Reporting API.",
    description = "Full documentation can be found [here](https://gitlab.com/meltano/tap-google-analytics)"
)
public class GoogleAnalytics extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @Schema(
        title = "Service account as json."
    )
    private Property<String> serviceAccount;

    @Schema(
        title = "OAuth Client ID"
    )
    private Property<String> oauthClientId;

    @Schema(
        title = "OAuth Client Secret."
    )
    private Property<String> oauthClientSecret;

    @Schema(
        title = "OAuth Access Token."
    )
    private Property<String> oauthAccessToken;

    @Schema(
        title = "OAuth Refresh Token."
    )
    private Property<String> oauthRefreshToken;

    @Schema(
        title = "OAuth Refresh Token."
    )
    private Property<String> view_id;

    @Schema(
        title = "Reports."
    )
    @PluginProperty(dynamic = true)
    private List<Report> reports;

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

    public List<Feature> features() {
        return Arrays.asList(
            Feature.CATALOG,
            Feature.DISCOVER
        );
    }

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("start_date", runContext.render(this.startDate.toString()));

        if (this.serviceAccount != null) {
            this.writeSingerFiles("google-credentials.json", runContext.render(this.serviceAccount).as(String.class).orElseThrow());
            builder.put("key_file_location", workingDirectory.toAbsolutePath() + "/google-credentials.json");
        }

        if (this.oauthClientId != null) {
            builder.put("oauth_credentials.client_id", runContext.render(this.oauthClientId).as(String.class).orElseThrow());
        }

        if (this.oauthClientSecret != null) {
            builder.put("oauth_credentials.client_secret", runContext.render(this.oauthClientSecret).as(String.class).orElseThrow());
        }

        if (this.oauthAccessToken != null) {
            builder.put("oauth_credentials.access_token", runContext.render(this.oauthAccessToken).as(String.class).orElseThrow());
        }

        if (this.oauthRefreshToken != null) {
            builder.put("oauth_credentials.refresh_token", runContext.render(this.oauthRefreshToken).as(String.class).orElseThrow());
        }

        if (this.view_id != null) {
            builder.put("view_id", runContext.render(this.view_id).as(String.class).orElseThrow());
        }

        if (this.reports != null) {
            this.writeSingerFiles("reports.json", this.reports);
            builder.put("reports", workingDirectory.toAbsolutePath() + "/reports.json");
        }

        if (this.endDate != null) {
            builder.put("end_date", runContext.render(this.endDate.toString()));
        }

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.of(Collections.singletonList("git+https://gitlab.com/meltano/tap-google-analytics.git"));
    }

    @Override
    protected Property<String> command() {
        return Property.of("tap-google-analytics");
    }

    @Getter
    @NoArgsConstructor
    public static class Report {
        private String name;
        private List<String> dimensions;
        private List<String> metrics;
    }
}
