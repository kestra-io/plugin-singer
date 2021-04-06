package io.kestra.plugin.singer.taps;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.singer.models.DiscoverMetadata;
import io.kestra.plugin.singer.models.StreamsConfiguration;
import io.kestra.plugin.singer.models.streams.AbstractStream;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@MicronautTest
class BigQueryTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        URL resource = BigQueryTest.class.getClassLoader().getResource("gcp-service-account.yml");
        String serviceAccount = CharStreams.toString(new InputStreamReader(new FileInputStream(Objects.requireNonNull(resource).getFile())));

        BigQuery.BigQueryBuilder<?, ?> builder = BigQuery.builder()
            .id(IdUtils.create())
            .type(ExchangeRateHost.class.getName())
            .serviceAccount(serviceAccount)
            .startDateTime(Instant.parse("2013-09-08T16:19:12Z"))
            .streams(Collections.singletonList(
                BigQuery.Stream.builder()
                    .name("covid19_nyt_us_states")
                    .table("bigquery-public-data.covid19_nyt.us_states")
                    .columns(Arrays.asList("date", "state_name", "confirmed_cases", "deaths"))
                    .datetimeKey("date")
                    .filters(Arrays.asList(
                        "EXTRACT (YEAR from date) = 2020",
                        "EXTRACT (MONTH from date) = 3",
                        "state_fips_code = '66'"
                    ))
                    .build()
            ))
            .streamsConfigurations(Collections.singletonList(
                StreamsConfiguration.builder()
                    .replicationMethod(DiscoverMetadata.ReplicationMethod.FULL_TABLE)
                    .build()
            ));

        BigQuery task = builder.build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        PipelinewiseMongoDb.Output output = task.run(runContext);

        assertThat(output.getSchemas().size(), is(1));
        assertThat(output.getSchemas().get("covid19_nyt_us_states"), is(notNullValue()));
        assertThat(output.getStreams().size(), is(1));
        assertThat(output.getStreams().get("covid19_nyt_us_states"), is(notNullValue()));
        assertThat(output.getCount().get(AbstractStream.Type.RECORD), is(17L));
        assertThat(output.getState(), is(notNullValue()));
    }
}
