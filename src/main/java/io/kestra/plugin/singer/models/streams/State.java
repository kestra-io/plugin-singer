package io.kestra.plugin.singer.models.streams;

import io.kestra.plugin.singer.taps.AbstractPythonTap;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.Map;

@SuperBuilder
@Getter
@NoArgsConstructor
public class State extends AbstractStream {
    private Map<String, Object> value;

    @Override
    public void onNext(AbstractPythonTap abstractPythonSinger) {
       abstractPythonSinger.stateMessage(this.value);
    }
}
