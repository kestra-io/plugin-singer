package io.kestra.plugin.singer.models.streams;

import io.kestra.plugin.singer.taps.AbstractPythonTap;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Getter
@NoArgsConstructor
public class ActivateVersion extends AbstractStream {
    private long version;

    @Override
    public void onNext(AbstractPythonTap abstractPythonSinger) {

    }
}
