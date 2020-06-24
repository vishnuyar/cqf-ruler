package org.opencds.cqf.common.retrieve;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.opencds.cqf.common.config.HapiProperties;
import org.opencds.cqf.cql.retrieve.*;
import org.opencds.cqf.cql.searchparam.SearchParameterResolver;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.jpa.dao.DaoRegistry;
import ca.uhn.fhir.jpa.dao.IFhirResourceDao;
import ca.uhn.fhir.jpa.searchparam.SearchParameterMap;
import ca.uhn.fhir.rest.api.CacheControlDirective;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.util.BundleUtil;
import ca.uhn.fhir.util.bundle.BundleEntryParts;

public class JpaFhirRetrieveProvider extends SearchParamFhirRetrieveProvider {
    org.slf4j.Logger ourLog = org.slf4j.LoggerFactory.getLogger("searchparam");
    DaoRegistry registry;
    HashMap<String,Collection<Object>>cacheQueries;
    public static ThreadLocal<String> patient_fhir;
    
    static {
        patient_fhir = new ThreadLocal<String>();
    	patient_fhir.set("");
    }

    public JpaFhirRetrieveProvider(DaoRegistry registry, SearchParameterResolver searchParameterResolver) {
        super(searchParameterResolver);
        this.registry = registry;
        cacheQueries = new HashMap<>();
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

    protected Collection<Object> oldExecuteQuery(String dataType, SearchParameterMap map) {
        IFhirResourceDao<?> dao = this.registry.getResourceDao(dataType);
        IBundleProvider bundleProvider = dao.search(map);

        ourLog.info("running Mapstring:" + dataType + map.toNormalizedQueryString(FhirContext.forR4()));
        if (bundleProvider.size() == null) {
            return resolveResourceList(bundleProvider.getResources(0, 10000));
        }
        if (bundleProvider.size() == 0) {
            return new ArrayList<>();
        }
        List<IBaseResource> resourceList = bundleProvider.getResources(0, bundleProvider.size());
        return resolveResourceList(resourceList);
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

    protected Collection<Object> executeQuery(String dataType, SearchParameterMap map) {
        // ourLog.info("Starting query");
        IFhirResourceDao<?> dao = this.registry.getResourceDao(dataType);
        
        FhirContext myFhirContext = FhirContext.forR4();
        boolean local = true;
        String purl = "";
        if (HapiProperties.getProperties().containsKey("patient_server_url")) {
            local = false;
            
            purl = HapiProperties.getProperties().getProperty("patient_server_url");
        }
        if (patient_fhir.get() != null) {

            
            if (!patient_fhir.get().equals("")) {
                local = false;
                purl = patient_fhir.get();
            }
        }
        String searchURL = "/" + dataType + map.toNormalizedQueryString(myFhirContext);
        if(cacheQueries.containsKey(searchURL)){
            ourLog.info("Retrieving from cache : "+searchURL);
            return cacheQueries.get(searchURL);
        }
        // System.out.println("The query string is " + searchURL);
        ourLog.info("The query string is "  + searchURL);
       
        IGenericClient client;
        List<IBaseResource> resourceList = new ArrayList<>();
        if (!local) {
            
            client = FhirContext.forR4().newRestfulGenericClient(purl);

            IBaseBundle bundle = client.search().byUrl(searchURL)
                    .cacheControl(new CacheControlDirective().setNoCache(true)).execute();
            List<BundleEntryParts> parts = BundleUtil.toListOfEntries(myFhirContext, bundle);
            for (Iterator<BundleEntryParts> iterator = parts.iterator(); iterator.hasNext();) {
                BundleEntryParts next = iterator.next();
                resourceList.add(next.getResource());
            }
        }
        // IGenericClient client = FhirContext.forR4()
        // .newRestfulGenericClient("http://localhost:9999/hapi-fhir-jpaserver/fhir");

        else {
            IBundleProvider bundleProvider = dao.search(map);
            if (bundleProvider.size() == null) {
                ourLog.info("Caching : "+searchURL);
                Collection<Object> queryResult = resolveResourceList(bundleProvider.getResources(0, 10000));
                //cacheQueries.put(searchURL,queryResult);
                return queryResult;
            }
            if (bundleProvider.size() == 0) {
                ourLog.info("Empty data ");
                //cacheQueries.put(searchURL,new ArrayList<>());
                return new ArrayList<>();
            }
            resourceList = bundleProvider.getResources(0, bundleProvider.size());

        }
       // ourLog.info("Leaving query");
       Collection<Object> queryResult = resolveResourceList(resourceList);
       ourLog.info("Caching : "+searchURL);
       //cacheQueries.put(searchURL,queryResult);
        return queryResult;
    }

}