package com.exampledb.talend.components.source;

import java.io.Serializable;

import com.exampledb.talend.components.dataset.ExampleDBDataset;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@GridLayout({
    // the generated layout put one configuration entry per line,
    // customize it as much as needed
    @GridLayout.Row({ "dataset" })
})
@Documentation("TODO fill the documentation for this configuration")
public class ExampleDBSourceMapperConfiguration implements Serializable {
    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private ExampleDBDataset dataset;

    public ExampleDBDataset getDataset() {
        return dataset;
    }

    public ExampleDBSourceMapperConfiguration setDataset(ExampleDBDataset dataset) {
        this.dataset = dataset;
        return this;
    }
}