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
    title = "A Singer tap to fetch data from a Salesforce account.",
    description = "Full documentation can be found [here](https://gitlab.com/meltano/tap-salesforce.git)"
)
public class Salesforce extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @Schema(
        title = "This is used to switch the behavior of the tap between using Salesforce's `REST` and `BULK` APIs."
    )
    @Builder.Default
    private final Property<ApiType> apiType = Property.of(ApiType.BULK);

    @NotNull
    @Schema(
        title = "Select by default any new fields discovered in Salesforce objects."
    )
    @Builder.Default
    private final Property<Boolean> selectFieldsByDefault = Property.of(true);

    @NotNull
    @Schema(
        title = "Select by default any new fields discovered in Salesforce objects."
    )
    @Builder.Default
    private final Property<Boolean> isSandbox = Property.of(false);

    @NotNull
    @Schema(
        title = "Generate a STATE message every N records."
    )
    @Builder.Default
    private final Property<Integer> stateMessageThreshold = Property.of(1000);

    @NotNull
    @Schema(
        title = "Maximum number of threads to use."
    )
    @Builder.Default
    private final Property<Integer> maxWorkers = Property.of(8);

    @Schema(
        title = "Salesforce username."
    )
    private Property<String> username;

    @Schema(
        title = "Salesforce password."
    )
    private Property<String> password;

    @Schema(
        title = "Your Salesforce Account access token."
    )
    private Property<String> securityToken;

    @Schema(
        title = "Salesforce client ID."
    )
    private Property<String> clientId;

    @Schema(
        title = "Salesforce client secret."
    )
    private Property<String> clientSecret;

    @Schema(
        title = "Salesforce refresh token."
    )
    private Property<String> refreshToken;

    @NotNull
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
            .put("api_type", runContext.render(this.apiType).as(ApiType.class).orElseThrow())
            .put("select_fields_by_default", runContext.render(this.selectFieldsByDefault).as(Boolean.class).orElseThrow())
            .put("is_sandbox", runContext.render(this.isSandbox).as(Boolean.class).orElseThrow())
            .put("state_message_threshold", runContext.render(this.stateMessageThreshold).as(Integer.class).orElseThrow())
            .put("max_workers", runContext.render(this.maxWorkers).as(Integer.class).orElseThrow())
            .put("start_date", runContext.render(this.startDate.toString()));

        if (this.username != null) {
            builder.put("username", runContext.render(this.username).as(String.class).orElseThrow());
        }

        if (this.password != null) {
            builder.put("password", runContext.render(this.password).as(String.class).orElseThrow());
        }

        if (this.securityToken != null) {
            builder.put("security_token", runContext.render(this.securityToken).as(String.class).orElseThrow());
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

        if (this.username != null) {
            builder.put("username", runContext.render(this.username).as(String.class).orElseThrow());
        }

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.of(Collections.singletonList("git+https://gitlab.com/meltano/tap-salesforce.git"));
    }

    @Override
    protected Property<String> command() {
        return Property.of("tap-salesforce");
    }

    public enum ApiType {
        REST,
        BULK
    }
}
