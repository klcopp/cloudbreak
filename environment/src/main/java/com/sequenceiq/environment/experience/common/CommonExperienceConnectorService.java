package com.sequenceiq.environment.experience.common;

import static com.sequenceiq.cloudbreak.client.AbstractUserCrnServiceEndpoint.CRN_HEADER;
import static com.sequenceiq.cloudbreak.util.ConditionBasedEvaluatorUtil.throwIfTrue;
import static com.sequenceiq.cloudbreak.util.NullUtil.throwIfNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.sequenceiq.cloudbreak.auth.ThreadBasedUserCrnProvider;
import com.sequenceiq.environment.experience.ResponseReader;
import com.sequenceiq.environment.experience.RetriableWebTarget;
import com.sequenceiq.environment.experience.api.CommonExperienceApi;
import com.sequenceiq.environment.experience.common.responses.CpInternalCluster;
import com.sequenceiq.environment.experience.common.responses.CpInternalEnvironmentResponse;
import com.sequenceiq.environment.experience.common.responses.DeleteCommonExperienceWorkspaceResponse;

@Service
public class CommonExperienceConnectorService implements CommonExperienceApi {

    private static final String COMMON_XP_RESPONSE_RESOLVE_ERROR_MSG = "Unable to resolve the experience's response!";

    private static final String INVALID_XP_BASE_PATH_GIVEN_MSG = "Experience base path should not be null!";

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonExperienceConnectorService.class);

    private static final String REQUEST_ID_HEADER = "x-cdp-request-id";

    private final RetriableWebTarget retriableWebTarget;

    private final String componentToReplaceInPath;

    private final ResponseReader responseReader;

    private final Client client;

    public CommonExperienceConnectorService(RetriableWebTarget retriableWebTarget, CommonExperienceResponseReader commonExperienceResponseReader,
            @Value("${experience.scan.path.componentToReplace}") String componentToReplaceInPath, Client client) {
        throwIfTrue(isEmpty(componentToReplaceInPath),
                () -> new IllegalArgumentException("Component what should be replaced in experience path must not be empty or null."));
        this.componentToReplaceInPath = componentToReplaceInPath;
        this.responseReader = commonExperienceResponseReader;
        this.retriableWebTarget = retriableWebTarget;
        this.client = client;
    }

    @Override
    @NotNull
    public Set<String> getWorkspaceNamesConnectedToEnv(String experienceBasePath, String environmentCrn) {
        WebTarget webTarget = createWebTargetBasedOnInputs(experienceBasePath, environmentCrn);
        Optional<Response> result = execCall(webTarget.getUri().toString(), () -> retriableWebTarget.get(createCompleteCallableBuilder(webTarget)));
        if (result.isPresent()) {
            Optional<CpInternalEnvironmentResponse> response = responseReader
                    .read(webTarget.getUri().toString(), result.get(), CpInternalEnvironmentResponse.class);
            return response.map(CommonExperienceConnectorService::getExperienceNamesFromListResponse).orElseGet(Set::of);
        }
        return Collections.emptySet();
    }

    @Override
    @NotNull
    public DeleteCommonExperienceWorkspaceResponse deleteWorkspaceForEnvironment(String experienceBasePath, String environmentCrn) {
        WebTarget webTarget = createWebTargetBasedOnInputs(experienceBasePath, environmentCrn);
        Optional<Response> response = execCall(webTarget.getUri().toString(), () -> retriableWebTarget.delete(createCompleteCallableBuilder(webTarget)));
        return responseReader.read(
                webTarget.getUri().toString(),
                response.orElseThrow(() -> new IllegalStateException(COMMON_XP_RESPONSE_RESOLVE_ERROR_MSG)),
                DeleteCommonExperienceWorkspaceResponse.class)
                .orElseThrow(() -> new IllegalStateException(COMMON_XP_RESPONSE_RESOLVE_ERROR_MSG));
    }

    private Optional<Response> execCall(String path, Callable<Response> toCall) {
        LOGGER.debug("About to connect to experience on path: {}", path);
        try {
            return Optional.ofNullable(toCall.call());
        } catch (Exception re) {
            LOGGER.warn("Something happened while the experience connection attempted!", re);
        }
        return Optional.empty();
    }

    private Invocation.Builder createCompleteCallableBuilder(WebTarget target) {
        return target
                .request()
                .accept(APPLICATION_JSON)
                .header(REQUEST_ID_HEADER, UUID.randomUUID().toString())
                .header(CRN_HEADER, ThreadBasedUserCrnProvider.getUserCrn());
    }

    private WebTarget createWebTargetBasedOnInputs(String experienceBasePath, String environmentCrn) {
        LOGGER.debug("Creating WebTarget to connect experience");
        return client.target(createPathToExperience(experienceBasePath, environmentCrn));
    }

    private String createPathToExperience(String experienceBasePath, String environmentCrn) {
        checkExperienceBasePath(experienceBasePath);
        return experienceBasePath.replace(componentToReplaceInPath, environmentCrn);
    }

    private void checkExperienceBasePath(String experienceBasePath) {
        throwIfNull(experienceBasePath, () -> new IllegalArgumentException(INVALID_XP_BASE_PATH_GIVEN_MSG));
    }

    private static Set<String> getExperienceNamesFromListResponse(CpInternalEnvironmentResponse experienceCallResponse) {
        return experienceCallResponse.getResults().stream().map(CpInternalCluster::getName).collect(Collectors.toSet());
    }

}
