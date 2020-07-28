package com.exampledb.talend.components.output;

import com.exampledb.talend.components.service.I18nMessage;

public final class QueryManagerFactory {

    private QueryManagerFactory() {
    }

    public static QueryManagerImpl getQueryManager(final I18nMessage i18n,
                                                   final ExampleDBOutputConfiguration configuration) {

        return new Insert(configuration, i18n);
    }

}
