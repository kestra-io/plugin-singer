package io.kestra.plugin.singer.targets;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.singer.AbstractPythonSinger;
import io.kestra.plugin.singer.models.Feature;
import io.kestra.plugin.singer.models.streams.AbstractStream;
import io.kestra.plugin.singer.taps.AbstractPythonTap;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.*;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractPythonTarget extends AbstractPythonSinger {
    private static final TypeReference<Map<String, Object>> TYPE_REFERENCE = new TypeReference<>() {};

    @Schema(
        title = "The source tap"
    )
    @NotNull
    @Valid
    private AbstractPythonTap tap;

    protected AbstractPythonTarget.Output runTarget(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        Path workingDirectory = this.tmpWorkingDirectory();

        // init working dir
        this.initVirtualEnv(runContext, logger);
        this.tap.init(runContext, logger);

        // sync
        Long syncResult = this.tapsSync(workingDirectory, runContext, logger);
        this.saveSingerMetrics(runContext);

        // outputs
        AbstractPythonTarget.Output.OutputBuilder builder = AbstractPythonTarget.Output.builder();

        if (this.tap.features().contains(Feature.STATE) && this.state.size() > 0) {
            builder.state(this.tap.saveState(runContext));
        }

        return builder.build();
    }

    protected Long tapsSync(Path workingDirectory, RunContext runContext, Logger logger) throws Exception {
        List<String> commands = new ArrayList<>(this.tap.tapCommand());

        commands.add("|");
        commands.add(workingDirectory + "/bin/" + this.command() + " --config " + workingDirectory + "/config.json ");

        return this.runSinger(commands, runContext, logger);
    }

    protected void runSingerCommand(List<String> commands, RunContext runContext, Logger logger, FlowableEmitter<String> emitter) throws Exception {
        this.run(
            runContext,
            logger,
            workingDirectory,
            this.finalCommandsWithInterpreter(String.join(" ", commands)),
            this.environnementVariable(runContext),
            this.logThreadSupplier(logger, emitter::onNext)
        );
    }

    protected Long runSinger(List<String> commands, RunContext runContext, Logger logger) {
        return Flowable
            .<String>create(
                emitter -> {
                    runSingerCommand(commands, runContext, logger, emitter);

                    emitter.onComplete();
                },
                BackpressureStrategy.BUFFER
            )
            .map(s -> MAPPER.readValue(s, TYPE_REFERENCE))
            .map(s -> {
                this.stateMessage(s);

                return this;
            })
            .count()
            .blockingGet();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Uri of the state file"
        )
        private final URI state;
    }
}
