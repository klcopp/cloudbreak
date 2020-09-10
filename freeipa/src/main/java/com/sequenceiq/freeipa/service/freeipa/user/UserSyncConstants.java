package com.sequenceiq.freeipa.service.freeipa.user;

import com.google.common.collect.ImmutableList;
import com.sequenceiq.authorization.resource.AuthorizationResourceAction;

import java.util.List;

public final class UserSyncConstants {

    public static final String ADMINS_GROUP = "admins";

    public static final String CDP_USERSYNC_INTERNAL_GROUP = "cdp-usersync-internal";

    public static final String ACCESS_ENVIRONMENT =
            AuthorizationResourceAction.ACCESS_ENVIRONMENT.getRight();

    public static final String ADMIN_FREEIPA =
            AuthorizationResourceAction.ADMIN_FREEIPA.getRight();

    public static final List<String> RIGHTS = ImmutableList.of(ACCESS_ENVIRONMENT, ADMIN_FREEIPA);

    private UserSyncConstants() {
        // private access
    }
}
