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
    title = "A Singer tap to fetch data from a Salesforce account.",
    description = "Full documentation can be found [here](https://gitlab.com/meltano/tap-salesforce.git)"
)
public class Salesforce extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @NotEmpty
    @Schema(
        title = "This is used to switch the behavior of the tap between using Salesforce's `REST` and `BULK` APIs."
    )
    @PluginProperty
    @Builder.Default
    private final ApiType apiType = ApiType.BULK;

    @NotNull
    @Schema(
        title = "Select by default any new fields discovered in Salesforce objects."
    )
    @PluginProperty
    @Builder.Default
    private final Boolean selectFieldsByDefault = true;

    @NotNull
    @Schema(
        title = "Select by default any new fields discovered in Salesforce objects."
    )
    @PluginProperty
    @Builder.Default
    private final Boolean isSandbox = false;

    @NotNull
    @Schema(
        title = "Generate a STATE message every N records."
    )
    @PluginProperty
    @Builder.Default
    private final Integer stateMessageThreshold = 1000;

    @NotNull
    @Schema(
        title = "Maximum number of threads to use."
    )
    @PluginProperty
    @Builder.Default
    private final Integer maxWorkers = 8;

    @Schema(
        title = "Salesforce username."
    )
    @PluginProperty(dynamic = true)
    private String username;

    @Schema(
        title = "Salesforce password."
    )
    @PluginProperty(dynamic = true)
    private String password;

    @Schema(
        title = "Your Salesforce Account access token."
    )
    @PluginProperty(dynamic = true)
    private String securityToken;

    @Schema(
        title = "Salesforce client ID."
    )
    @PluginProperty(dynamic = true)
    private String clientId;

    @Schema(
        title = "Salesforce client secret."
    )
    @PluginProperty(dynamic = true)
    private String clientSecret;

    @Schema(
        title = "Salesforce refresh token."
    )
    @PluginProperty(dynamic = true)
    private String refreshToken;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Determines how much historical data will be extracted.",
        description = "Please be aware that the larger the time period and amount of data, the longer the initial extraction can be expected to take."
    )
    @PluginProperty(dynamic = true)
    private LocalDate startDate;

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
            .put("api_type", this.apiType)
            .put("select_fields_by_default", this.selectFieldsByDefault)
            .put("is_sandbox", this.isSandbox)
            .put("state_message_threshold", this.stateMessageThreshold)
            .put("max_workers", this.maxWorkers)
            .put("start_date", runContext.render(this.startDate.toString()));

        if (this.username != null) {
            builder.put("username", runContext.render(this.username));
        }

        if (this.password != null) {
            builder.put("password", runContext.render(this.password));
        }

        if (this.securityToken != null) {
            builder.put("security_token", runContext.render(this.securityToken));
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

        if (this.username != null) {
            builder.put("username", runContext.render(this.username));
        }

        if (this.username != null) {
            builder.put("username", runContext.render(this.username));
        }

        return builder.build();
    }

    @Override
    public List<String> pipPackages() {
        return Collections.singletonList("git+https://gitlab.com/meltano/tap-salesforce.git");
    }

    @Override
    protected String command() {
        return "tap-salesforce";
    }

    public enum ApiType {
        REST,
        BULK
    }
}
