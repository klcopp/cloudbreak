package com.sequenceiq.it.cloudbreak.action.freeipa;

import static java.lang.String.format;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sequenceiq.freeipa.api.v1.freeipa.user.model.SyncOperationStatus;
import com.sequenceiq.it.cloudbreak.FreeIpaClient;
import com.sequenceiq.it.cloudbreak.context.TestContext;
import com.sequenceiq.it.cloudbreak.dto.freeipa.FreeIpaUserSyncTestDto;
import com.sequenceiq.it.cloudbreak.log.Log;

public class FreeIpaGetLastSyncStatusAction extends AbstractFreeIpaAction<FreeIpaUserSyncTestDto> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FreeIpaGetLastSyncStatusAction.class);

    @Override
    protected FreeIpaUserSyncTestDto freeIpaAction(TestContext testContext, FreeIpaUserSyncTestDto testDto, FreeIpaClient client) throws Exception {
        Log.when(LOGGER, format(" Environment Crn: [%s], freeIpa Crn: %s", testDto.getEnvironmentCrn(), testDto.getRequest().getEnvironments()));
        Log.whenJson(LOGGER, format(" FreeIPA get last sync status request: %n"), testDto.getRequest());
        SyncOperationStatus syncOperationStatus = client.getFreeIpaClient()
                .getUserV1Endpoint()
                .getLastSyncOperationStatus(testDto.getEnvironmentCrn());
        testDto.setOperationId(syncOperationStatus.getOperationId());
        LOGGER.info("Last sync is in state: [{}], last sync operation: [{}]", syncOperationStatus.getStatus(),
                OBJECT_MAPPER.writeValueAsString(syncOperationStatus));
        Log.when(LOGGER, format(" Last sync is in state: [%s], last sync operation: [%s]", syncOperationStatus.getStatus(),
                OBJECT_MAPPER.writeValueAsString(syncOperationStatus)));
        return testDto;
    }
}
