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
    title = "Fetch data from a Netsuite account with a Singer tap.",
    description = "Full documentation can be found on the [GitHub Repo](https://github.com/hotgluexyz/tap-netsuite)."
)
public class Netsuite extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @NotEmpty
    @Schema(
        title = "Your account ID.",
        description = "This can be found under Setup -> Company -> Company Information. Look for Account Id. Note `_SB` is for Sandbox account."
    )
    @PluginProperty(dynamic = true)
    private String accountId;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Your consumer key for token based authentication consumer key for SOAP connection.",
        description = "Visit [this page](https://support.cazoomi.com/hc/en-us/articles/360010093392-How-to-Setup-NetSuite-Token-Based-Authentication-as-Authentication-Type) for details."
    )
    @PluginProperty(dynamic = true)
    private String consumerKey;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Your consumer secret for token based authentication consumer key for SOAP connection.",
        description = "Visit [this page](https://support.cazoomi.com/hc/en-us/articles/360010093392-How-to-Setup-NetSuite-Token-Based-Authentication-as-Authentication-Type) for details."
    )
    @PluginProperty(dynamic = true)
    private String consumerSecret;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Your token key for token based authentication consumer key for SOAP connection.",
        description = "Visit [this page](https://support.cazoomi.com/hc/en-us/articles/360010093392-How-to-Setup-NetSuite-Token-Based-Authentication-as-Authentication-Type) for details."
    )
    @PluginProperty(dynamic = true)
    private String tokenKey;

    @NotNull
    @NotEmpty
    @Schema(
        title = "our token secret for token based authentication consumer key for SOAP connection.",
        description = "Visit [this page](https://support.cazoomi.com/hc/en-us/articles/360010093392-How-to-Setup-NetSuite-Token-Based-Authentication-as-Authentication-Type) for details."
    )
    @PluginProperty(dynamic = true)
    private String tokenSecret;

    @NotNull
    @Schema(
        title = "Behaviour when new fields are discovered.",
        description = "When new fields are discovered in NetSuite objects, the select_fields_by_default key describes whether or not the tap will select those fields by default."
    )
    private Property<Boolean> selectFieldsByDefault;

    @NotNull
    @Schema(
        title = "Is this sandbox account.",
        description = "This should always be set to `true` if you are connecting Production account of NetSuite. Set it to `false` if you want to connect to SandBox account."
    )
    private Property<Boolean> isSandbox;

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
            .put("ns_account", runContext.render(this.accountId))
            .put("ns_consumer_key", runContext.render(this.consumerKey))
            .put("ns_consumer_secret", runContext.render(this.consumerSecret))
            .put("ns_token_key", runContext.render(this.tokenKey))
            .put("ns_token_secret", runContext.render(this.tokenSecret))
            .put("select_fields_by_default", runContext.render(this.selectFieldsByDefault).as(Boolean.class).orElseThrow())
            .put("is_sandbox", runContext.render(this.isSandbox).as(Boolean.class).orElseThrow())
            .put("start_date", runContext.render(this.startDate.toString()));

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.of(Collections.singletonList("git+https://github.com/hotgluexyz/tap-netsuite"));
    }

    @Override
    protected Property<String> command() {
        return Property.of("tap-netsuite");
    }
}
