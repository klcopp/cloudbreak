package com.sequenceiq.freeipa.client.operation;

import static com.sequenceiq.freeipa.client.FreeIpaClient.MAX_PASSWORD_EXPIRATION_DATETIME;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sequenceiq.freeipa.client.FreeIpaChecks;
import com.sequenceiq.freeipa.client.FreeIpaClient;
import com.sequenceiq.freeipa.client.FreeIpaClientException;
import com.sequenceiq.freeipa.client.model.User;

public class UserAddOperation extends AbstractFreeipaOperation<User> {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserAddOperation.class);

    private String user;

    private String firstName;

    private String lastName;

    private UserAddOperation(String user, String firstName, String lastName) {
        this.user = user;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    public static UserAddOperation get(String user, String firstName, String lastName) {
        return new UserAddOperation(user, firstName, lastName);
    }

    @Override
    public String getMethodName() {
        return "user_add";
    }

    @Override
    protected List<Object> getFlags() {
        return List.of(user);
    }

    @Override
    protected Map<String, Object> getParams() {
        return Map.of(
                "givenname", firstName,
                "sn", lastName,
                "loginshell", "/bin/bash",
                "random", true,
                "setattr", "krbPasswordExpiration=" + MAX_PASSWORD_EXPIRATION_DATETIME
        );
    }

    public Optional<User> invoke(FreeIpaClient freeIpaClient) throws FreeIpaClientException {
        FreeIpaChecks.checkUserNotProtected(user, () -> String.format("User '%s' is protected and cannot be added to FreeIPA", user));
        LOGGER.debug("adding user {}", user);
        User user = invoke(freeIpaClient, User.class);
        LOGGER.debug("added user {}", user);
        return Optional.of(user);
    }
}
