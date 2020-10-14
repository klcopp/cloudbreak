package com.sequenceiq.environment.environment.validation.network.azure;

import static com.sequenceiq.cloudbreak.common.mappable.CloudPlatform.AZURE;
import static com.sequenceiq.common.api.type.ServiceEndpointCreation.ENABLED_PRIVATE_ENDPOINT;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.sequenceiq.cloudbreak.cloud.azure.AzureCloudSubnetParametersService;
import com.sequenceiq.cloudbreak.cloud.model.CloudSubnet;
import com.sequenceiq.cloudbreak.common.mappable.CloudPlatform;
import com.sequenceiq.cloudbreak.validation.ValidationResult.ValidationResultBuilder;
import com.sequenceiq.common.api.type.ServiceEndpointCreation;
import com.sequenceiq.environment.environment.dto.EnvironmentDto;
import com.sequenceiq.environment.environment.validation.network.EnvironmentNetworkValidator;
import com.sequenceiq.environment.network.CloudNetworkService;
import com.sequenceiq.environment.network.dto.AzureParams;
import com.sequenceiq.environment.network.dto.NetworkDto;
import com.sequenceiq.environment.parameters.dao.domain.ResourceGroupUsagePattern;
import com.sequenceiq.environment.parameters.dto.AzureParametersDto;
import com.sequenceiq.environment.parameters.dto.AzureResourceGroupDto;
import com.sequenceiq.environment.parameters.dto.ParametersDto;

@Component
public class AzureEnvironmentNetworkValidator implements EnvironmentNetworkValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureEnvironmentNetworkValidator.class);

    private final CloudNetworkService cloudNetworkService;

    private final AzureCloudSubnetParametersService azureCloudSubnetParametersService;

    public AzureEnvironmentNetworkValidator(CloudNetworkService cloudNetworkService, AzureCloudSubnetParametersService azureCloudSubnetParametersService) {
        this.cloudNetworkService = cloudNetworkService;
        this.azureCloudSubnetParametersService = azureCloudSubnetParametersService;
    }

    @Override
    public void validateDuringFlow(EnvironmentDto environmentDto, NetworkDto networkDto, ValidationResultBuilder resultBuilder) {
        if (environmentDto == null || networkDto == null) {
            LOGGER.warn("EnvironmentDto or NetworkDto. Neither them can be null!");
            resultBuilder.error("Internal validation error");
            return;
        }
        Map<String, CloudSubnet> cloudNetworks = cloudNetworkService.retrieveSubnetMetadata(environmentDto, networkDto);
        checkSubnetsProvidedWhenExistingNetwork(resultBuilder, networkDto, networkDto.getAzure(), cloudNetworks);
        checkPrivateEndpointNetworkPoliciesWhenExistingNetwork(networkDto, cloudNetworks, resultBuilder);
        checkPrivateEndpointsWhenMultipleResourceGroup(resultBuilder, environmentDto, networkDto.getServiceEndpointCreation());
    }

    @Override
    public void validateDuringRequest(NetworkDto networkDto, ValidationResultBuilder resultBuilder) {
        if (networkDto == null) {
            return;
        }

        if (StringUtils.isEmpty(networkDto.getNetworkCidr()) && StringUtils.isEmpty(networkDto.getNetworkId())) {
            String message = "Either the AZURE network id or cidr needs to be defined!";
            LOGGER.info(message);
            resultBuilder.error(message);
        }

        AzureParams azureParams = networkDto.getAzure();
        if (azureParams != null) {
            checkSubnetsProvidedWhenExistingNetwork(resultBuilder, azureParams, networkDto.getSubnetMetas());
            checkExistingNetworkParamsProvidedWhenSubnetsPresent(networkDto, resultBuilder);
            checkResourceGroupNameWhenExistingNetwork(resultBuilder, azureParams);
            checkNetworkIdWhenExistingNetwork(resultBuilder, azureParams);
            checkNetworkIdIsSpecifiedWhenSubnetIdsArePresent(resultBuilder, azureParams, networkDto);
        } else if (StringUtils.isEmpty(networkDto.getNetworkCidr())) {
            resultBuilder.error(missingParamsErrorMsg(AZURE));
        }
    }

    private void checkSubnetsProvidedWhenExistingNetwork(ValidationResultBuilder resultBuilder, NetworkDto network,
            AzureParams azureParams, Map<String, CloudSubnet> subnetMetas) {
        if (StringUtils.isNotEmpty(azureParams.getNetworkId()) && StringUtils.isNotEmpty(azureParams.getResourceGroupName())) {
            if (CollectionUtils.isEmpty(network.getSubnetIds())) {
                String message = String.format("If networkId (%s) and resourceGroupName (%s) are specified then subnet ids must be specified as well.",
                        azureParams.getNetworkId(), azureParams.getResourceGroupName());
                LOGGER.info(message);
                resultBuilder.error(message);
            } else if (subnetMetas.size() != network.getSubnetIds().size()) {
                String message = String.format("If networkId (%s) and resourceGroupName (%s) are specified then subnet ids must be specified and should exist " +
                                "on azure as well. Given subnetids: [%s], exisiting ones: [%s]", azureParams.getNetworkId(), azureParams.getResourceGroupName(),
                        network.getSubnetIds().stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(", ")),
                        subnetMetas.keySet().stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(", ")));
                LOGGER.info(message);
                resultBuilder.error(message);
            }
        }
    }

    private void checkPrivateEndpointNetworkPoliciesWhenExistingNetwork(NetworkDto networkDto, Map<String, CloudSubnet> cloudNetworks, ValidationResultBuilder resultBuilder) {
        if (!ENABLED_PRIVATE_ENDPOINT.equals(networkDto.getServiceEndpointCreation())) {
            LOGGER.debug("No private endpoint network policies validation requested");
            return;
        }

        if (StringUtils.isEmpty(networkDto.getNetworkId()) || StringUtils.isEmpty(networkDto.getAzure().getResourceGroupName())) {
            LOGGER.debug("Using new network -- bypassing private endpoint network policies validation");
            return;
        }

        if (cloudNetworks.values().stream().noneMatch(azureCloudSubnetParametersService::isPrivateEndpointNetworkPoliciesDisabled)) {
            String errorMessage = String.format("It is not possible to create private endpoints: existing network with id '%s' in resource group '%s' " +
                            "has no subnet with privateEndpointNetworkPolicies disabled.",
                    networkDto.getNetworkId(), networkDto.getAzure().getResourceGroupName());
            LOGGER.warn(errorMessage);
            resultBuilder.error(errorMessage);
        }
    }

    private void checkResourceGroupNameWhenExistingNetwork(ValidationResultBuilder resultBuilder, AzureParams azureParams) {
        if (StringUtils.isNotEmpty(azureParams.getNetworkId()) && StringUtils.isEmpty(azureParams.getResourceGroupName())) {
            resultBuilder.error("If networkId is specified, then resourceGroupName must be specified too.");
        }
    }

    private void checkNetworkIdWhenExistingNetwork(ValidationResultBuilder resultBuilder, AzureParams azureParams) {
        if (StringUtils.isEmpty(azureParams.getNetworkId()) && StringUtils.isNotEmpty(azureParams.getResourceGroupName())) {
            resultBuilder.error("If resourceGroupName is specified, then networkId must be specified too.");
        }
    }

    private void checkExistingNetworkParamsProvidedWhenSubnetsPresent(NetworkDto networkDto, ValidationResultBuilder resultBuilder) {
        if (!networkDto.getSubnetIds().isEmpty()
                && StringUtils.isEmpty(networkDto.getAzure().getNetworkId())
                && StringUtils.isEmpty(networkDto.getAzure().getResourceGroupName())) {
            String message =
                    String.format("If %s subnet ids were provided then network id and resource group name have to be specified, too.", AZURE.name());
            LOGGER.info(message);
            resultBuilder.error(message);
        }
    }

    private void checkNetworkIdIsSpecifiedWhenSubnetIdsArePresent(ValidationResultBuilder resultBuilder,
            AzureParams azureParams, NetworkDto networkDto) {
        if (StringUtils.isEmpty(azureParams.getNetworkId()) && CollectionUtils.isNotEmpty(networkDto.getSubnetIds())) {
            resultBuilder.error("If subnetIds are specified, then networkId must be specified too.");
        }
    }

    private void checkSubnetsProvidedWhenExistingNetwork(ValidationResultBuilder resultBuilder,
            AzureParams azureParams, Map<String, CloudSubnet> subnetMetas) {
        if (StringUtils.isNotEmpty(azureParams.getNetworkId()) && StringUtils.isNotEmpty(azureParams.getResourceGroupName())
                && MapUtils.isEmpty(subnetMetas)) {
            String message = String.format("If networkId (%s) and resourceGroupName (%s) are specified then subnet ids must be specified as well.",
                    azureParams.getNetworkId(), azureParams.getResourceGroupName());
            LOGGER.info(message);
            resultBuilder.error(message);
        }
    }

    private void checkPrivateEndpointsWhenMultipleResourceGroup(ValidationResultBuilder resultBuilder, EnvironmentDto environmentDto,
            ServiceEndpointCreation serviceEndpointCreation) {
        ResourceGroupUsagePattern resourceGroupUsagePattern = Optional.ofNullable(environmentDto.getParameters())
                .map(ParametersDto::azureParametersDto)
                .map(AzureParametersDto::getAzureResourceGroupDto)
                .map(AzureResourceGroupDto::getResourceGroupUsagePattern)
                .orElse(ResourceGroupUsagePattern.USE_MULTIPLE);
        if (resourceGroupUsagePattern == ResourceGroupUsagePattern.USE_MULTIPLE
                && serviceEndpointCreation == ServiceEndpointCreation.ENABLED_PRIVATE_ENDPOINT) {
            resultBuilder.error("Private endpoint creation is not supported for multiple resource group deployment model, "
                    + "please use single single resource groups to be able to use private endpoints in Azure!");
        }
    }

    @Override
    public CloudPlatform getCloudPlatform() {
        return AZURE;
    }

}
