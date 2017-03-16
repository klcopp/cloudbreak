package com.sequenceiq.cloudbreak.api.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.sequenceiq.cloudbreak.doc.ModelDescriptions;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel
@JsonIgnoreProperties(ignoreUnknown = true)
public class SmartSenseSubscriptionJson implements JsonEntity {

    @ApiModelProperty(value = ModelDescriptions.ID, readOnly = true)
    private Long id;

    @ApiModelProperty(value = ModelDescriptions.SmartSenseSubscriptionModelDescription.SUBSCRIPTION_ID, required = true)
    private String subscriptionId;

    @ApiModelProperty(value = ModelDescriptions.OWNER, readOnly = true)
    private String owner;

    @ApiModelProperty(value = ModelDescriptions.ACCOUNT, readOnly = true)
    private String account;

    @ApiModelProperty(value = ModelDescriptions.PUBLIC_IN_ACCOUNT, readOnly = true)
    private boolean publicInAccount;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public void setSubscriptionId(String subscriptionId) {
        this.subscriptionId = subscriptionId;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public boolean isPublicInAccount() {
        return publicInAccount;
    }

    public void setPublicInAccount(boolean publicInAccount) {
        this.publicInAccount = publicInAccount;
    }
}
