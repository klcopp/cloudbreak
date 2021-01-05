package com.sequenceiq.freeipa.service.freeipa;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.cloudera.thunderhead.service.usermanagement.UserManagementProto;
import com.google.common.collect.Lists;
import com.sequenceiq.freeipa.client.FreeIpaClient;
import com.sequenceiq.freeipa.client.FreeIpaClientException;
import com.sequenceiq.freeipa.client.FreeIpaClientExceptionUtil;
import com.sequenceiq.freeipa.client.operation.SetWlCredentialOperation;
import com.sequenceiq.freeipa.service.freeipa.user.kerberos.KrbKeySetEncoder;
import com.sequenceiq.freeipa.service.freeipa.user.model.WorkloadCredential;

@Service
public class WorkloadCredentialService {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkloadCredentialService.class);

    public void setWorkloadCredential(FreeIpaClient freeIpaClient, String username, WorkloadCredential workloadCredential)
            throws IOException, FreeIpaClientException {
        LOGGER.debug("Setting workload credentials for user '{}'", username);
        try {
            String expiration = freeIpaClient.formatDate(workloadCredential.getExpirationDate());
            getOperation(username, workloadCredential, expiration).invoke(freeIpaClient);
        } catch (FreeIpaClientException e) {
            if (FreeIpaClientExceptionUtil.isEmptyModlistException(e)) {
                LOGGER.debug("Workload credentials for user '{}' already set.", username);
            } else {
                throw e;
            }
        }
    }

    public void setWorkloadCredentials(boolean fmsToFreeipaBatchCallEnabled, FreeIpaClient freeIpaClient, Map<String, WorkloadCredential> workloadCredentials,
            BiConsumer<String, String> warnings) throws FreeIpaClientException, IOException {
        List<Object> operations = Lists.newArrayList();
        for (Map.Entry<String, WorkloadCredential> entry : workloadCredentials.entrySet()) {
            if (fmsToFreeipaBatchCallEnabled) {
                String username = entry.getKey();
                WorkloadCredential workloadCredential = entry.getValue();
                String expiration = freeIpaClient.formatDate(workloadCredential.getExpirationDate());
                getOperation(username, workloadCredential, expiration).appendOperationToBatchCall(operations);
            } else {
                try {
                    setWorkloadCredential(freeIpaClient, entry.getKey(), entry.getValue());
                } catch (IOException e) {
                    recordWarning(entry.getKey(), e, warnings);
                } catch (FreeIpaClientException e) {
                    recordWarning(entry.getKey(), e, warnings);
                    if (e.isClientUnusable()) {
                        LOGGER.warn("Client is not usable for further usage");
                        throw e;
                    }
                }
            }
        }
        if (fmsToFreeipaBatchCallEnabled) {
            freeIpaClient.callBatch(warnings, operations, Set.of());
        }
    }

    private SetWlCredentialOperation getOperation(String user, WorkloadCredential workloadCredential, String expiration) throws IOException {
        String asnEncodedKrbPrincipalKey = KrbKeySetEncoder.getASNEncodedKrbPrincipalKey(workloadCredential.getKeys());
        List<String> sshPublicKeys = workloadCredential.getSshPublicKeys().stream()
                .map(UserManagementProto.SshPublicKey::getPublicKey).collect(Collectors.toList());
        return SetWlCredentialOperation.get(user, workloadCredential.getHashedPassword(), asnEncodedKrbPrincipalKey, sshPublicKeys, expiration);
    }

    private void recordWarning(String username, Exception e, BiConsumer<String, String> warnings) {
        LOGGER.warn("Failed to set workload credentials for user '{}'", username, e);
        warnings.accept(username, "Failed to set workload credentials:" + e.getMessage());
    }
}