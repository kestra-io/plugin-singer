package io.kestra.plugin.singer.targets;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.singer.AbstractPythonSinger;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
        title = "The raw data from a tap"
    )
    @NotNull
    @Valid
    private String from;

    protected AbstractPythonTarget.Output runTarget(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        if (this.workingDirectory == null) {
            this.workingDirectory = runContext.tempDir();
        }

        this.initVirtualEnv(runContext, logger);

        // from
        URI from = new URI(runContext.render(this.from));
        Path tempFile = runContext.tempFile();
        Files.copy(runContext.uriToInputStream(from), tempFile, StandardCopyOption.REPLACE_EXISTING);

        // sync
        this.tapsSync(workingDirectory, tempFile, runContext, logger);
        this.saveSingerMetrics(runContext);

        // outputs
        AbstractPythonTarget.Output.OutputBuilder builder = AbstractPythonTarget.Output.builder();

        if (this.stateRecords.size() > 0) {
            builder.state(this.saveState(runContext, this.stateName, this.stateRecords));
        }

        return builder.build();
    }

    protected Long tapsSync(Path workingDirectory, Path tempFile, RunContext runContext, Logger logger) throws IllegalVariableEvaluationException {
        List<String> commands = new ArrayList<>(List.of("cat " + tempFile.toAbsolutePath()));

        commands.add("|");
        commands.add(workingDirectory + "/bin/" + this.finalCommand(runContext) + " --config " + workingDirectory + "/config.json ");

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
