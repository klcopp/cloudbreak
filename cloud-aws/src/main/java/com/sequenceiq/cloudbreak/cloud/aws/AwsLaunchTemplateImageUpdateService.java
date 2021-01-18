package com.sequenceiq.cloudbreak.cloud.aws;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.amazonaws.services.cloudformation.AmazonCloudFormationClient;
import com.amazonaws.services.cloudformation.model.GetTemplateRequest;
import com.amazonaws.services.cloudformation.model.GetTemplateResult;
import com.amazonaws.services.cloudformation.model.UpdateStackRequest;
import com.sequenceiq.cloudbreak.cloud.aws.view.AwsCredentialView;
import com.sequenceiq.cloudbreak.cloud.aws.view.AwsGroupView;
import com.sequenceiq.cloudbreak.cloud.context.AuthenticatedContext;
import com.sequenceiq.cloudbreak.cloud.model.CloudResource;
import com.sequenceiq.cloudbreak.cloud.model.CloudStack;
import com.sequenceiq.cloudbreak.common.json.Json;

@Service
public class AwsLaunchTemplateImageUpdateService {

    @Inject
    private AwsClient awsClient;

    @Inject
    private AwsStackRequestHelper awsStackRequestHelper;

    public void updateImage(AuthenticatedContext authenticatedContext, CloudStack stack, CloudResource cfResource) {
        AwsCredentialView credentialView = new AwsCredentialView(authenticatedContext.getCloudCredential());
        String regionName = authenticatedContext.getCloudContext().getLocation().getRegion().getRegionName();
        AmazonCloudFormationClient cloudFormationClient = awsClient.createCloudFormationClient(credentialView, regionName);

        String imageName = stack.getImage().getImageName();
        String cfStackName = cfResource.getName();
        String cfTemplate = getCfTemplate(cfResource, cloudFormationClient);
        Json templateJson = new Json(cfTemplate);

        stack.getGroups().forEach(group -> {
            String imageIdPath = String.format("Resources.%s.Properties.LaunchTemplateData.ImageId", AwsGroupView.getLaunchTemplateName(group.getName()));
            Object oldImageId = templateJson.getValue(imageIdPath);
            boolean encryptedImage = !"{\"Ref\":\"AMI\"}".equals(oldImageId.toString());
            if (encryptedImage) {
                replaceEncryptedImageInGroup(imageName, templateJson, imageIdPath);
            }
        });

        String newCfTemplate = templateJson.getValue();
        UpdateStackRequest updateStackRequest = awsStackRequestHelper.createUpdateStackRequest(authenticatedContext, stack, cfStackName, newCfTemplate);
        cloudFormationClient.updateStack(updateStackRequest);
    }

    private String getCfTemplate(CloudResource cfResource, AmazonCloudFormationClient cloudFormationClient) {
        String cfStackName = cfResource.getName();
        GetTemplateResult template = cloudFormationClient.getTemplate(new GetTemplateRequest().withStackName(cfStackName));
        return template.getTemplateBody();
    }

    private void replaceEncryptedImageInGroup(String imageName, Json templateJson, String imageIdPath) {
        templateJson.replaceValue(imageIdPath, imageName);
    }
}
