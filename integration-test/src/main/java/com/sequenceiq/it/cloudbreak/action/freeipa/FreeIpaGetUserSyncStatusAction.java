package com.sequenceiq.it.cloudbreak.action.freeipa;

import static java.lang.String.format;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sequenceiq.freeipa.api.v1.freeipa.user.model.EnvironmentUserSyncState;
import com.sequenceiq.it.cloudbreak.FreeIpaClient;
import com.sequenceiq.it.cloudbreak.context.TestContext;
import com.sequenceiq.it.cloudbreak.dto.freeipa.FreeIpaUserSyncTestDto;
import com.sequenceiq.it.cloudbreak.log.Log;

public class FreeIpaGetUserSyncStatusAction extends AbstractFreeIpaAction<FreeIpaUserSyncTestDto> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FreeIpaGetUserSyncStatusAction.class);

    @Override
    protected FreeIpaUserSyncTestDto freeIpaAction(TestContext testContext, FreeIpaUserSyncTestDto testDto, FreeIpaClient client) throws Exception {
        Log.when(LOGGER, format(" Environment Crn: [%s], freeIpa Crn: [%s]", testDto.getEnvironmentCrn(), testDto.getRequest().getEnvironments()));
        Log.whenJson(LOGGER, format(" Environment user sync status request: %n"), testDto.getRequest());
        EnvironmentUserSyncState syncOperationStatus = client.getFreeIpaClient()
                .getUserV1Endpoint()
                .getUserSyncState(testDto.getEnvironmentCrn());
        testDto.setOperationId(syncOperationStatus.getLastUserSyncOperationId());
        LOGGER.info("User sync is in state: [{}], last user sync operation: [{}]", syncOperationStatus.getState(),
                OBJECT_MAPPER.writeValueAsString(syncOperationStatus));
        Log.when(LOGGER, format(" User sync is in state: [%s], last user sync operation: [%s]", syncOperationStatus.getState(),
                OBJECT_MAPPER.writeValueAsString(syncOperationStatus)));
        return testDto;
    }
}
