package com.exampledb.talend.components.datastore;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

@DataStore("ExampleDBDatastore")
@GridLayout({
    // the generated layout put one configuration entry per line,
    // customize it as much as needed
    @GridLayout.Row({ "server" }),
    @GridLayout.Row({ "port" }),
    @GridLayout.Row({ "database" }),
    @GridLayout.Row({ "username" }),
    @GridLayout.Row({ "password" })
})

@Checkable("validateConnection")
@Documentation("TODO fill the documentation for this configuration")
public class ExampleDBDatastore implements Serializable {
    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String server;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private int port;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String database;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String username;

    @Credential
    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String password;

    public String getServer() {
        return server;
    }

    public ExampleDBDatastore setServer(String server) {
        this.server = server;
        return this;
    }

    public int getPort() {
        return port;
    }

    public ExampleDBDatastore setPort(int port) {
        this.port = port;
        return this;
    }

    public String getDatabase() {
        return database;
    }

    public ExampleDBDatastore setDatabase(String database) {
        this.database = database;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public ExampleDBDatastore setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public ExampleDBDatastore setPassword(String password) {
        this.password = password;
        return this;
    }
}