package com.sequenceiq.cloudbreak.reactor.api.event.externaldatabase;

import com.sequenceiq.cloudbreak.core.flow2.externaldatabase.ExternalDatabaseSelectableEvent;

public class StartExternalDatabaseResult extends ExternalDatabaseSelectableEvent {

    public StartExternalDatabaseResult(Long resourceId, String selector, String resourceName, String resourceCrn) {
        super(resourceId, selector, resourceName, resourceCrn);
    }

    @Override
    public String selector() {
        return "StartExternalDatabaseResult";
    }
}
