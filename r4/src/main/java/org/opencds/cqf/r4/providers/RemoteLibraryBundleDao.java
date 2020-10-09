package org.opencds.cqf.r4.providers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;

import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseMetaType;
import org.hl7.fhir.instance.model.api.IBaseParameters;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.r4.model.Library;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.jpa.api.dao.IFhirResourceDao;
import ca.uhn.fhir.jpa.api.model.DaoMethodOutcome;
import ca.uhn.fhir.jpa.api.model.DeleteConflictList;
import ca.uhn.fhir.jpa.api.model.DeleteMethodOutcome;
import ca.uhn.fhir.jpa.api.model.ExpungeOptions;
import ca.uhn.fhir.jpa.api.model.ExpungeOutcome;
import ca.uhn.fhir.jpa.model.entity.BaseHasResource;
import ca.uhn.fhir.jpa.model.entity.IBaseResourceEntity;
import ca.uhn.fhir.jpa.model.entity.ResourceTable;
import ca.uhn.fhir.jpa.model.entity.ResourceTag;
import ca.uhn.fhir.jpa.model.entity.TagTypeEnum;
import ca.uhn.fhir.jpa.searchparam.SearchParameterMap;
import ca.uhn.fhir.rest.api.CacheControlDirective;
import ca.uhn.fhir.rest.api.EncodingEnum;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.api.PatchTypeEnum;
import ca.uhn.fhir.rest.api.ValidationModeEnum;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.api.server.storage.ResourcePersistentId;
import ca.uhn.fhir.rest.api.server.storage.TransactionDetails;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BearerTokenAuthInterceptor;
import ca.uhn.fhir.rest.server.SimpleBundleProvider;
import ca.uhn.fhir.util.BundleUtil;
import ca.uhn.fhir.util.bundle.BundleEntryParts;

public class RemoteLibraryBundleDao implements IFhirResourceDao<Library> {

    private String fhirServerUrl;
    private String fhirServerToken;
    private FhirContext fhirContext;

    public RemoteLibraryBundleDao(String serverUrl, String serverToken) {
        this.fhirServerUrl = serverUrl;
        this.fhirServerToken = serverToken;
        this.fhirContext = FhirContext.forR4();
    }

    @Override
    public FhirContext getContext() {
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
    public void addTag(IIdType theId, TagTypeEnum theTagType, String theScheme, String theTerm, String theLabel,
            RequestDetails theRequest) {
        // TODO Auto-generated method stub

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
    public Library read(IIdType theId) {
        try {
            IGenericClient client = getClient();
            System.out.println("id: " + theId.getIdPart());

            String searchURL = this.fhirServerUrl + "Library/" + theId.getValue();
            System.out.println("the url being contacted is : " + searchURL);
            return client.fetchResourceFromUrl(Library.class, searchURL);
        } catch (Exception e) {
            e.printStackTrace();

        }

        return null;
    }

    private IGenericClient getClient() {
        IGenericClient client;
        client = this.fhirContext.newRestfulGenericClient(this.fhirServerUrl);
        if (this.fhirServerToken != null) {
            if (!this.fhirServerToken.equals("")) {
                BearerTokenAuthInterceptor authInterceptor = new BearerTokenAuthInterceptor(this.fhirServerToken);
                client.registerInterceptor(authInterceptor);
            }
        }
        return client;
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
    public IBundleProvider search(SearchParameterMap map) {
        try {
            List<IBaseResource> resourceList = new ArrayList<>();
            IGenericClient client = getClient();
            String searchURL = this.fhirServerUrl + "Library" + map.toNormalizedQueryString(this.fhirContext);
            System.out.println("the url being contacted is : " + searchURL);
            IBaseBundle bundle = client.search().byUrl(searchURL)
                    .cacheControl(new CacheControlDirective().setNoCache(true)).execute();
            List<BundleEntryParts> parts = BundleUtil.toListOfEntries(this.fhirContext, bundle);
            if (parts.size() > 0) {
                for (Iterator<BundleEntryParts> iterator = parts.iterator(); iterator.hasNext();) {
                    BundleEntryParts next = iterator.next();
                    resourceList.add(next.getResource());
                }

            }

            SimpleBundleProvider sb = new SimpleBundleProvider(resourceList);
            return sb;

        } catch (Exception e) {
            e.printStackTrace();

        }
        return null;

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
            TransactionDetails theTransactionDetails, RequestDetails theRequestDetails) {
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
            RequestDetails theRequestDetails, TransactionDetails theTransactionDetails) {
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
    public IBundleProvider search(SearchParameterMap theParams, RequestDetails theRequestDetails) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IBundleProvider search(SearchParameterMap theParams, RequestDetails theRequestDetails,
            HttpServletResponse theServletResponse) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Set<ResourcePersistentId> searchForIds(SearchParameterMap theParams, RequestDetails theRequest) {
        // TODO Auto-generated method stub
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
            boolean theForceUpdateVersion, RequestDetails theRequestDetails, TransactionDetails theTransactionDetails) {
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

    @Override
    public DaoMethodOutcome patch(IIdType arg0, String arg1, PatchTypeEnum arg2, String arg3, IBaseParameters arg4,
            RequestDetails arg5) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Library readByPid(ResourcePersistentId arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DeleteMethodOutcome deletePidList(String arg0, Collection<ResourcePersistentId> arg1,
            DeleteConflictList arg2, RequestDetails arg3) {
        // TODO Auto-generated method stub
        return null;
    }

}