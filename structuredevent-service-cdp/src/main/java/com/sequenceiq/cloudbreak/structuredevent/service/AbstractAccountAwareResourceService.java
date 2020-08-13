package com.sequenceiq.cloudbreak.structuredevent.service;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;

import org.hibernate.exception.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;

import com.google.common.collect.Sets;
import com.sequenceiq.cloudbreak.logger.MDCBuilder;
import com.sequenceiq.cloudbreak.structuredevent.repository.AccountAwareResource;
import com.sequenceiq.cloudbreak.structuredevent.repository.AccountAwareResourceRepository;

public abstract class AbstractAccountAwareResourceService<T extends AccountAwareResource>
        implements AccountAwareResourceService<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAccountAwareResourceService.class);

    @Inject
    private CDPBaseRestRequestThreadLocalService cdpBaseRestRequestThreadLocalService;

    @Override
    public T create(T resource, @Nonnull String accountId) {
        return create(resource, accountId);
    }

    @Override
    public T createWithMdcContextRestore(T resource, String accountId) {
        Map<String, String> mdcContextMap = MDCBuilder.getMdcContextMap();
        try {
            return createInternal(resource, accountId);
        } finally {
            MDCBuilder.buildMdcContextFromMap(mdcContextMap);
        }
    }

    private T createInternal(T resource, String accountId) {
        try {
            MDCBuilder.buildMdcContext(resource);
            prepareCreation(resource);
            return repository().save(resource);
        } catch (DataIntegrityViolationException | ConstraintViolationException e) {
            String message = String.format("%s already exists with name '%s' in account %s",
                    resource.getResourceName(), resource.getName(), accountId);
            throw new BadRequestException(message, e);
        }
    }

    @Override
    public T getByNameForAccountId(String name, String accountId) {
        Optional<T> object = repository().findByNameAndAccountId(name, accountId);
        if (object.isEmpty()) {
            throw new NotFoundException(String.format("No resource found with name '%s'", name));
        }
        MDCBuilder.buildMdcContext(object.get());
        return object.get();
    }

    @Override
    public Set<T> getByNamesForAccountId(Set<String> names, String accountId) {
        Set<T> results = repository().findByNameInAndAccountId(names, accountId);
        Set<String> notFound = Sets.difference(names,
                results.stream().map(AccountAwareResource::getName).collect(Collectors.toSet()));

        if (!notFound.isEmpty()) {
            throw new NotFoundException(String.format("No resource(s) found with name(s) '%s'",
                    notFound.stream().map(name -> '\'' + name + '\'').collect(Collectors.joining(", "))));
        }

        return results;
    }

    @Override
    public T getByNameForWorkspaceId(String name, String accountId) {
        Optional<T> object = repository().findByNameAndAccountId(name, accountId);
        if (object.isEmpty()) {
            throw new NotFoundException(String.format("No resource found with name '%s'", name));
        }
        MDCBuilder.buildMdcContext(object.get());
        return object.get();
    }

    @Override
    public Set<T> findAllByAccountId(String accountId) {
        return repository().findAllByAccountId(accountId);
    }

    @Override
    public T deleteWithMdcContextRestore(T resource) {
        Map<String, String> mdcContextMap = MDCBuilder.getMdcContextMap();
        try {
            return deleteInternal(resource);
        } finally {
            MDCBuilder.buildMdcContextFromMap(mdcContextMap);
        }
    }

    @Override
    public T delete(T resource) {
        return deleteInternal(resource);
    }

    private T deleteInternal(T resource) {
        MDCBuilder.buildMdcContext(resource);
        LOGGER.debug("Deleting {} with name: {}", resource.getResourceName(), resource.getName());
        prepareDeletion(resource);
        repository().delete(resource);
        return resource;
    }

    @Override
    public Set<T> delete(Set<T> resources) {
        return resources.stream()
                .map(this::delete)
                .collect(Collectors.toSet());
    }

    @Override
    public T deleteByNameFromAccountId(String name, String accountId) {
        T toBeDeleted = getByNameForAccountId(name, accountId);
        return delete(toBeDeleted);
    }

    @Override
    public Set<T> deleteMultipleByNameFromAccountId(Set<String> names, String accountId) {
        Set<T> toBeDeleted = getByNamesForAccountId(names, accountId);
        return delete(toBeDeleted);
    }

    @Override
    public Iterable<T> findAll() {
        return repository().findAll();
    }

    @Override
    public T pureSave(T resource) {
        return repository().save(resource);
    }

    @Override
    public Iterable<T> pureSaveAll(Iterable<T> resources) {
        return repository().saveAll(resources);
    }

    protected abstract AccountAwareResourceRepository<T, Long> repository();

    protected abstract void prepareDeletion(T resource);

    protected abstract void prepareCreation(T resource);

}
