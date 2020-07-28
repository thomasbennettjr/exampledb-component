package com.exampledb.talend.components.dataset;

import java.io.Serializable;

import com.exampledb.talend.components.datastore.ExampleDBDatastore;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@DataSet("ExampleDBDataset")
@GridLayout({
    // the generated layout put one configuration entry per line,
    // customize it as much as needed
    @GridLayout.Row({ "datastore" }),
    @GridLayout.Row({ "table" })
})
@Documentation("TODO fill the documentation for this configuration")
public class ExampleDBDataset implements Serializable {
    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private ExampleDBDatastore datastore;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    @Required
    @Suggestable(value="listTables", parameters="datastore")
    private String table;

    public ExampleDBDatastore getDatastore() {
        return datastore;
    }

    public ExampleDBDataset setDatastore(ExampleDBDatastore datastore) {
        this.datastore = datastore;
        return this;
    }

    public String getTable() {
        return table;
    }

    public ExampleDBDataset setTable(String table) {
        this.table = table;
        return this;
    }
}