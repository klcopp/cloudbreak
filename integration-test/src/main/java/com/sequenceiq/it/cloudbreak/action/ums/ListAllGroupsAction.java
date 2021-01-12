package com.sequenceiq.it.cloudbreak.action.ums;

import static java.lang.String.format;

import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.thunderhead.service.usermanagement.UserManagementProto;
import com.sequenceiq.it.cloudbreak.UmsClient;
import com.sequenceiq.it.cloudbreak.action.Action;
import com.sequenceiq.it.cloudbreak.actor.CloudbreakUser;
import com.sequenceiq.it.cloudbreak.actor.CloudbreakUserCache;
import com.sequenceiq.it.cloudbreak.context.TestContext;
import com.sequenceiq.it.cloudbreak.dto.ums.UmsTestDto;
import com.sequenceiq.it.cloudbreak.log.Log;

public class ListAllGroupsAction implements Action<UmsTestDto, UmsClient> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ListAllGroupsAction.class);

    private final String userKey;

    public ListAllGroupsAction(String userKey) {
        this.userKey = userKey;
    }

    @Override
    public UmsTestDto action(TestContext testContext, UmsTestDto testDto, UmsClient client) throws Exception {
        CloudbreakUser user = CloudbreakUserCache.getInstance().getByName(userKey);
        Log.when(LOGGER, format(" List all UMS groups at: %s with role: %s and user: %s", testDto.getRequest().getResourceCrn(),
                testDto.getRequest().getRoleCrn(), user.getCrn()));
        Log.whenJson(LOGGER, format(" Retrieves list of all groups from UMS: %n"), testDto.getRequest());
        List<UserManagementProto.Group> listAllGroups = client.getUmsClient()
                .listAllGroups(user.getCrn(), testContext.getActingUserCrn().getAccountId(), Optional.of(""));
        testDto.setAllGroups(listAllGroups);
        return testDto;
    }
}
