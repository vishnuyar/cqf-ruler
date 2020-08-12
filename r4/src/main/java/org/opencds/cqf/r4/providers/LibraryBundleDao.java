package org.opencds.cqf.r4.providers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.servlet.http.HttpServletResponse;

import org.hl7.fhir.instance.model.api.IBaseMetaType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Library;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.jpa.dao.DaoMethodOutcome;
import ca.uhn.fhir.jpa.dao.DeleteMethodOutcome;
import ca.uhn.fhir.jpa.dao.IFhirResourceDao;
import ca.uhn.fhir.jpa.dao.ISearchBuilder;
import ca.uhn.fhir.jpa.delete.DeleteConflictList;
import ca.uhn.fhir.jpa.model.entity.BaseHasResource;
import ca.uhn.fhir.jpa.model.entity.IBaseResourceEntity;
import ca.uhn.fhir.jpa.model.entity.ResourceTable;
import ca.uhn.fhir.jpa.model.entity.ResourceTag;
import ca.uhn.fhir.jpa.model.entity.TagTypeEnum;
import ca.uhn.fhir.jpa.search.PersistedJpaBundleProvider;
import ca.uhn.fhir.jpa.searchparam.SearchParameterMap;
import ca.uhn.fhir.jpa.searchparam.registry.ISearchParamRegistry;
import ca.uhn.fhir.jpa.util.ExpungeOptions;
import ca.uhn.fhir.jpa.util.ExpungeOutcome;
import ca.uhn.fhir.model.api.IQueryParameterType;
import ca.uhn.fhir.rest.api.EncodingEnum;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.api.PatchTypeEnum;
import ca.uhn.fhir.rest.api.ValidationModeEnum;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.SimpleBundleProvider;

public class LibraryBundleDao implements IFhirResourceDao<Library> {

    private Bundle libBundle;

    public LibraryBundleDao(Bundle libBundle) {
        this.libBundle = libBundle;
    }

    @Override
    public FhirContext getContext() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void injectDependenciesIntoBundleProvider(PersistedJpaBundleProvider theProvider) {
        // TODO Auto-generated method stub

    }

    @Override
    public ISearchBuilder newSearchBuilder() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IBaseResource toResource(BaseHasResource theEntity, boolean theForHistoryOperation) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <R extends IBaseResource> R toResource(Class<R> theResourceType, IBaseResourceEntity theEntity,
            Collection<ResourceTag> theTagList, boolean theForHistoryOperation) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ISearchParamRegistry getSearchParamRegistry() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void addTag(IIdType theId, TagTypeEnum theTagType, String theScheme, String theTerm, String theLabel,
            RequestDetails theRequest) {
        // TODO Auto-generated method stub

    }

    @Override
    public DaoMethodOutcome create(Library theResource) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DaoMethodOutcome create(Library theResource, RequestDetails theRequestDetails) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DaoMethodOutcome create(Library theResource, String theIfNoneExist) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DaoMethodOutcome create(Library theResource, String theIfNoneExist, boolean thePerformIndexing,
            Date theUpdateTimestamp, RequestDetails theRequestDetails) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DaoMethodOutcome create(Library theResource, String theIfNoneExist, RequestDetails theRequestDetails) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DaoMethodOutcome delete(IIdType theResource) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DaoMethodOutcome delete(IIdType theResource, DeleteConflictList theDeleteConflictsListToPopulate,
            RequestDetails theRequestDetails) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DaoMethodOutcome delete(IIdType theResource, RequestDetails theRequestDetails) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DeleteMethodOutcome deleteByUrl(String theUrl, DeleteConflictList theDeleteConflictsListToPopulate,
            RequestDetails theRequestDetails) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DeleteMethodOutcome deleteByUrl(String theString, RequestDetails theRequestDetails) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ExpungeOutcome expunge(ExpungeOptions theExpungeOptions, RequestDetails theRequestDetails) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ExpungeOutcome expunge(IIdType theIIdType, ExpungeOptions theExpungeOptions, RequestDetails theRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ExpungeOutcome forceExpungeInExistingTransaction(IIdType theId, ExpungeOptions theExpungeOptions,
            RequestDetails theRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Class<Library> getResourceType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IBundleProvider history(Date theSince, Date theUntil, RequestDetails theRequestDetails) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IBundleProvider history(IIdType theId, Date theSince, Date theUntil, RequestDetails theRequestDetails) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <MT extends IBaseMetaType> MT metaAddOperation(IIdType theId1, MT theMetaAdd,
            RequestDetails theRequestDetails) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <MT extends IBaseMetaType> MT metaDeleteOperation(IIdType theId1, MT theMetaDel,
            RequestDetails theRequestDetails) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <MT extends IBaseMetaType> MT metaGetOperation(Class<MT> theType, IIdType theId,
            RequestDetails theRequestDetails) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <MT extends IBaseMetaType> MT metaGetOperation(Class<MT> theType, RequestDetails theRequestDetails) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DaoMethodOutcome patch(IIdType theId, String theConditionalUrl, PatchTypeEnum thePatchType,
            String thePatchBody, RequestDetails theRequestDetails) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Set<Long> processMatchUrl(String theMatchUrl, RequestDetails theRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Library read(IIdType theId) {
        System.out.println("Getting library id from bundle");
        for (Bundle.BundleEntryComponent entry : this.libBundle.getEntry()) {
            if (entry.getResource().getResourceType().toString().equals("Library")) {
                try {
                    System.out.println("id value:" + theId.getValue());
                    Library library = (Library) entry.getResource();
                    String libraryId = library.getId();
                    if (libraryId != null) {
                        String idPart = library.getIdElement().getIdPart();
                        if (theId.getValue().equals(idPart)) {
                            System.out.println("Library found");
                            return library;
                        }
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }
        return null;
    }

    @Override
    public IBaseResource readByPid(Long thePid) {
        // TODO Auto-generated method stub
        System.out.println("Search 6");
        return null;
    }

    @Override
    public Library read(IIdType theId, RequestDetails theRequestDetails) {
        // TODO Auto-generated method stub
        System.out.println("Search 7");
        return null;
    }

    @Override
    public Library read(IIdType theId, RequestDetails theRequestDetails, boolean theDeletedOk) {
        // TODO Auto-generated method stub
        System.out.println("Search 8");
        return null;
    }

    @Override
    public BaseHasResource readEntity(IIdType theId, RequestDetails theRequest) {
        // TODO Auto-generated method stub
        System.out.println("Search 9");
        return null;
    }

    @Override
    public BaseHasResource readEntity(IIdType theId, boolean theCheckForForcedId, RequestDetails theRequest) {
        // TODO Auto-generated method stub
        System.out.println("Search 1");
        return null;
    }

    @Override
    public void reindex(Library theResource, ResourceTable theEntity) {
        // TODO Auto-generated method stub

    }

    @Override
    public void removeTag(IIdType theId, TagTypeEnum theTagType, String theSystem, String theCode,
            RequestDetails theRequestDetails) {
        // TODO Auto-generated method stub

    }

    @Override
    public void removeTag(IIdType theId, TagTypeEnum theTagType, String theSystem, String theCode) {
        // TODO Auto-generated method stub

    }

    @Override
    public IBundleProvider search(SearchParameterMap theParams) {
        // TODO Auto-generated method
        List<IBaseResource> resources = new ArrayList<>();
        for (List<List<IQueryParameterType>> nextAnds : theParams.values()) {
            for (List<? extends IQueryParameterType> nextOrs : nextAnds) {
                for (IQueryParameterType next : nextOrs) {
                    String searchPara = next.getValueAsQueryToken(FhirContext.forR4());
                    System.out.println("next:: " + searchPara);
                    for (Bundle.BundleEntryComponent entry : this.libBundle.getEntry()) {
                        if (entry.getResource().getResourceType().toString().equals("Library")) {
                            Library library = (Library) entry.getResource();
                            String libName = library.getName();
                            System.out.println("lib name: " + libName);
                            if (searchPara.equals(libName)) {
                                System.out.println("Library found");
                                resources.add(library);
                            }
                        }
                    }
                    if (next.getMissing() != null) {
                        throw new ca.uhn.fhir.rest.server.exceptions.MethodNotAllowedException(
                                ":missing modifier is disabled on this server");
                    }
                }
            }
        }
        UUID uuid = UUID.randomUUID();
        System.out.println("Search 10");
        SimpleBundleProvider sb = new SimpleBundleProvider(resources);

        return sb;
    }

    @Override
    public IBundleProvider search(SearchParameterMap theParams, RequestDetails theRequestDetails) {
        // TODO Auto-generated method stub
        System.out.println("Search 2");
        return null;
    }

    @Override
    public IBundleProvider search(SearchParameterMap theParams, RequestDetails theRequestDetails,
            HttpServletResponse theServletResponse) {
        // TODO Auto-generated method stub
        System.out.println("Search 3");
        return null;
    }

    @Override
    public Set<Long> searchForIds(SearchParameterMap theParams, RequestDetails theRequest) {
        // TODO Auto-generated method stub
        System.out.println("Search 4");
        return null;
    }

    @Override
    public void translateRawParameters(Map<String, List<String>> theSource, SearchParameterMap theTarget) {
        // TODO Auto-generated method stub

    }

    @Override
    public DaoMethodOutcome update(Library theResource) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DaoMethodOutcome update(Library theResource, RequestDetails theRequestDetails) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DaoMethodOutcome update(Library theResource, String theMatchUrl) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DaoMethodOutcome update(Library theResource, String theMatchUrl, boolean thePerformIndexing,
            RequestDetails theRequestDetails) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DaoMethodOutcome update(Library theResource, String theMatchUrl, RequestDetails theRequestDetails) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DaoMethodOutcome update(Library theResource, String theMatchUrl, boolean thePerformIndexing,
            boolean theForceUpdateVersion, RequestDetails theRequestDetails) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MethodOutcome validate(Library theResource, IIdType theId, String theRawResource, EncodingEnum theEncoding,
            ValidationModeEnum theMode, String theProfile, RequestDetails theRequestDetails) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RuntimeResourceDefinition validateCriteriaAndReturnResourceDefinition(String criteria) {
        // TODO Auto-generated method stub
        return null;
    }

}