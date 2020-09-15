package org.opencds.cqf.common.retrieve;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.opencds.cqf.cql.engine.fhir.retrieve.SearchParamFhirRetrieveProvider;
import org.opencds.cqf.cql.engine.fhir.searchparam.SearchParameterMap;
import org.opencds.cqf.cql.engine.fhir.searchparam.SearchParameterResolver;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.jpa.dao.DaoRegistry;
import ca.uhn.fhir.rest.api.CacheControlDirective;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BearerTokenAuthInterceptor;
import ca.uhn.fhir.util.BundleUtil;
import ca.uhn.fhir.util.bundle.BundleEntryParts;

public class RemoteRetrieveProvider extends SearchParamFhirRetrieveProvider {
    org.slf4j.Logger ourLog = org.slf4j.LoggerFactory.getLogger("remotefhirsearch");
    DaoRegistry registry;
    public static ThreadLocal<HashMap> patient_fhir;
    static {
        patient_fhir = new ThreadLocal<HashMap>();
        HashMap<String, Object> nonLocal = new HashMap<>();
        patient_fhir.set(nonLocal);
    }

    public RemoteRetrieveProvider(DaoRegistry registry, SearchParameterResolver searchParameterResolver) {
        super(searchParameterResolver);
        this.registry = registry;

    }

    public synchronized Collection<Object> resolveResourceList(List<IBaseResource> resourceList) {
        List<Object> ret = new ArrayList<>();
        ourLog.info("size is: " + resourceList.size());
        for (IBaseResource res : resourceList) {
            Class<?> clazz = res.getClass();
            ret.add(clazz.cast(res));
        }
        // ret.addAll(resourceList);
        return ret;
    }

    @Override
    protected Iterable<Object> executeQueries(String dataType, List<SearchParameterMap> queries) {
        if (queries == null || queries.isEmpty()) {
            return Collections.emptyList();
        }

        List<Object> objects = new ArrayList<>();
        for (SearchParameterMap map : queries) {
            objects.addAll(executeQuery(dataType, map));
        }

        return objects;
    }

    protected Collection<Object> executeQuery(String dataType, SearchParameterMap map) {
        String purl = "";
        FhirContext myFhirContext = FhirContext.forR4();

        List<IBaseResource> resourceList = new ArrayList<>();

        String searchURL = "/" + dataType + map.toNormalizedQueryString(myFhirContext);
        ourLog.info("The query string is " + searchURL);
        if (patient_fhir.get().containsKey("patient_server_url")) {
            String nonlocal_url = (String) patient_fhir.get().get("patient_server_url");
            if (!nonlocal_url.equals("")) {
                purl = nonlocal_url;
            }
        }

        IGenericClient client;
        System.out.println("Remotefhir url :" + purl);
        if (purl.length() > 1) {

            client = FhirContext.forR4().newRestfulGenericClient(purl);

            // Check for token
            if (patient_fhir.get().containsKey("patient_server_token")) {
                String auth_token = (String) patient_fhir.get().get("patient_server_token");
                if (!auth_token.equals("")) {
                    BearerTokenAuthInterceptor authInterceptor = new BearerTokenAuthInterceptor(auth_token);
                    client.registerInterceptor(authInterceptor);
                }

            }

            try {
                while (searchURL != null) {

                    IBaseBundle bundle = client.search().byUrl(searchURL)
                            .cacheControl(new CacheControlDirective().setNoCache(true)).execute();
                    List<BundleEntryParts> parts = BundleUtil.toListOfEntries(myFhirContext, bundle);
                    if (parts.size() > 0) {
                        for (Iterator<BundleEntryParts> iterator = parts.iterator(); iterator.hasNext();) {
                            BundleEntryParts next = iterator.next();
                            resourceList.add(next.getResource());
                        }

                    }
                    searchURL = BundleUtil.getLinkUrlOfType(myFhirContext, bundle, bundle.LINK_NEXT);
                    // System.out.println("next: " + searchURL);
                }
            }

            catch (Exception e) {
                e.printStackTrace();

            }

        }

        Collection<Object> queryResult = resolveResourceList(resourceList);
        return queryResult;
    }

}