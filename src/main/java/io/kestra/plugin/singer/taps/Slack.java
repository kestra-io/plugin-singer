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
    title = "A Singer tap to fetch data from Slack.",
    description = "Full documentation can be found [here](https://github.com/Mashey/tap-slack)"
)
public class Slack extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @NotEmpty
    @Schema(
        title = "Slack Access Token.",
        description = "More details on Slack Access Tokens can be found [here](https://slack.dev/python-slack-sdk/installation/)."
    )
    @PluginProperty(dynamic = true)
    private String apiToken;

    @NotNull
    @Schema(
        title = "Determines how much historical data will be extracted.",
        description = "Please be aware that the larger the time period and amount of data, the longer the initial extraction can be expected to take."
    )
    @PluginProperty(dynamic = true)
    private LocalDate startDate;

    @Schema(
        title = "Join Private Channels.",
        description = "Specifies whether to sync private channels or not."
    )
    @Builder.Default
    private final Property<Boolean> privateChannels = Property.of(true);

    @Schema(
        title = "Join Public Channels.",
        description = "Specifies whether to have the tap auto-join all public channels in your ogranziation."
    )
    @Builder.Default
    private final Property<Boolean> publicChannels = Property.of(false);

    @Schema(
        title = "Sync Archived Channels.",
        description = "Specifies whether the tap will sync archived channels or not. Note that a bot cannot join " +
            "an archived channel, so unless the bot was added to the channel prior to it being archived it will not " +
            "be able to sync the data from that channel."
    )
    @Builder.Default
    private final Property<Boolean> archivedChannels = Property.of(false);

    @Schema(
        title = "Channels to Sync.",
        description = "By default the tap will sync all channels it has been invited to, but this can be overridden " +
            "to limit it to specific channels. Note this needs to be channel ID, not the name, as recommended " +
            "by the Slack API. To get the ID for a channel, either use the Slack API or find it in the URL."
    )
    private Property<List<String>> channels;

    @Schema(
        title = "Channels to Sync.",
        description = "Due to the potentially high volume of data when syncing certain streams " +
            "(messages, files, threads) this tap implements date windowing based on a configuration parameter." +
            "5 means the tap to sync 5 days of data per request, for applicable streams."
    )
    @Builder.Default
    private final Property<Integer> dateWindowSize = Property.of(7);

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
            .put("token", runContext.render(this.apiToken))
            .put("start_date", runContext.render(this.startDate.toString()))
            .put("private_channels", runContext.render(this.privateChannels).as(Boolean.class).orElseThrow())
            .put("join_public_channels", runContext.render(this.publicChannels).as(Boolean.class).orElseThrow())
            .put("archived_channels", runContext.render(this.archivedChannels).as(Boolean.class).orElseThrow());

        var renderedChannels = runContext.render(this.channels).asList(String.class);
        if (!renderedChannels.isEmpty()) {
            builder.put("channels", renderedChannels);
        }

        if (dateWindowSize != null) {
            builder.put("date_window_size", runContext.render(this.dateWindowSize).as(Integer.class).orElseThrow());
        }

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.of(Collections.singletonList("git+https://github.com/Mashey/tap-slack.git"));
    }

    @Override
    protected Property<String> command() {
        return Property.of("tap-slack");
    }
}
