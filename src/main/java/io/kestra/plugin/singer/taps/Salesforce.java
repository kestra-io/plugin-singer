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
    title = "Fetch data from a Salesforce account with a Singer tap.",
    description = "Full documentation can be found on the [GitHub Repo](https://github.com/singer-io/tap-salesforce)."
)
public class Salesforce extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @Schema(
        title = "This is used to switch the behavior of the tap between using Salesforce's `REST` and `BULK` APIs."
    )
    @Builder.Default
    private final Property<ApiType> apiType = Property.ofValue(ApiType.BULK);

    @NotNull
    @Schema(
        title = "Select by default any new fields discovered in Salesforce objects."
    )
    @Builder.Default
    private final Property<Boolean> selectFieldsByDefault = Property.ofValue(true);

    @NotNull
    @Schema(
        title = "Select by default any new fields discovered in Salesforce objects."
    )
    @Builder.Default
    private final Property<Boolean> isSandbox = Property.ofValue(false);

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

    @Schema(
        title = "The lookback_window.",
        description = "(in seconds) subtracts the desired amount of seconds from the bookmark to sync past data. Recommended value: 10 seconds."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private Property<Integer> lookbackWindow = Property.ofValue(10);

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
            .put("start_date", runContext.render(this.startDate.toString()));

        if (this.clientId != null) {
            builder.put("client_id", runContext.render(this.clientId).as(String.class).orElseThrow());
        }

        if (this.clientSecret != null) {
            builder.put("client_secret", runContext.render(this.clientSecret).as(String.class).orElseThrow());
        }

        if (this.refreshToken != null) {
            builder.put("refresh_token", runContext.render(this.refreshToken).as(String.class).orElseThrow());
        }

        if (this.lookbackWindow != null) {
            builder.put("lookback_window", runContext.render(this.lookbackWindow).as(Integer.class).orElseThrow());
        }

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.ofValue(Collections.singletonList("tap-salesforce"));
    }

    @Override
    protected Property<String> command() {
        return Property.ofValue("tap-salesforce");
    }

    public enum ApiType {
        REST,
        BULK
    }
}
