package org.opencds.cqf.common.retrieve;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Coverage;
import org.hl7.fhir.r4.model.DeviceRequest;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Immunization;
import org.hl7.fhir.r4.model.Medication;
import org.hl7.fhir.r4.model.MedicationAdministration;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Procedure;
import org.hl7.fhir.r4.model.ServiceRequest;
import org.opencds.cqf.cql.retrieve.SearchParamFhirRetrieveProvider;
import org.opencds.cqf.cql.searchparam.SearchParameterResolver;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.jpa.dao.DaoRegistry;
import ca.uhn.fhir.jpa.searchparam.SearchParameterMap;
import ca.uhn.fhir.model.api.IQueryParameterType;
import ca.uhn.fhir.util.BundleUtil;
import ca.uhn.fhir.util.bundle.BundleEntryParts;

public class InMemoryRetrieveProvider extends SearchParamFhirRetrieveProvider {
    org.slf4j.Logger ourLog = org.slf4j.LoggerFactory.getLogger("inmemorysearch");
    DaoRegistry registry;
    private FhirContext myFhirContext = FhirContext.forR4();
    private static String deepSearch [] = {"Patient", "Encounter","Procedure","Observation","Condition",
        "ServiceRequest","DeviceRequest","Medication","MedicationAdministration","Immunization","Coverage"};
        
    public static ThreadLocal<HashMap> patient_fhir;
    static {
        patient_fhir = new ThreadLocal<HashMap>();
        HashMap<String, Object> nonLocal = new HashMap<>();
        patient_fhir.set(nonLocal);
    }

    public InMemoryRetrieveProvider(DaoRegistry registry, SearchParameterResolver searchParameterResolver) {
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

    private ArrayList<String> getSearchParaSet(List<List<IQueryParameterType>> searchQuery) {
        ArrayList<String> searchParas = new ArrayList<>();
        for (List<? extends IQueryParameterType> nextOrs : searchQuery) {
            for (IQueryParameterType next : nextOrs) {
                String searchPara = next.getValueAsQueryToken(myFhirContext);
                searchParas.add(searchPara);
                if (next.getMissing() != null) {
                    throw new ca.uhn.fhir.rest.server.exceptions.MethodNotAllowedException(
                            ":missing modifier is disabled on this server");
                }
            }
        }
        return searchParas;

    }

    private void printMapKeys(SearchParameterMap map) {
        System.out.println("Printing keys in search map");
        for (String key : map.keySet()) {
            System.out.println("key:" + key);

        }
    }

    private Boolean isCodeAvailable(CodeableConcept code, ArrayList<String> searchCodes) {
        for (Coding coding : code.getCoding()) {
            String resourceCode = coding.getSystem() + "|" + coding.getCode();
            ourLog.info("code from resource: " + resourceCode);
            if (searchCodes.contains(resourceCode)) {
                return true;
            }
        }
        return false;
    }

    private Boolean checkCode(CodeableConcept code, SearchParameterMap map) {
        if (map.containsKey("code")) {
            ArrayList<String> codes = getSearchParaSet(map.get("code"));
            if (isCodeAvailable(code, codes)) {
                return true;
            }
        } else {
            return true;
        }
        return false;

    }

    private Boolean getValidResource(String dataType, SearchParameterMap map, IBaseResource resource) {
        Boolean validResource = false;
        CodeableConcept code = null;

        if (dataType.equals("Patient")) {
            Patient evalResource = (Patient) resource;
            if (map.containsKey("_id")) {
                String patientId = evalResource.getIdElement().getIdPart();
                ArrayList<String> subjects = getSearchParaSet(map.get("_id"));
                if (subjects.contains(patientId)) {
                    return true;
                }
            }
        } else if (dataType.equals("Encounter")) {
            Encounter evalResource = (Encounter) resource;
            List<CodeableConcept> Listcode = evalResource.getType();
            System.out.println("Reference:" + evalResource.getSubject().getReference());
            if (map.containsKey("type")) {
                ArrayList<String> encounterTypes = getSearchParaSet(map.get("type"));
                for (CodeableConcept codeConcept : Listcode) {
                    if (isCodeAvailable(codeConcept, encounterTypes)) {
                        return true;
                    }
                }
            } else {
                // if the key is not of type, then any encounter statisfies the search.
                return true;
            }
        } else if (dataType.equals("Procedure")) {
            Procedure evalResource = (Procedure) resource;
            System.out.println("Reference:" + evalResource.getSubject().getReference());
            code = evalResource.getCode();
            return checkCode(code, map);

        } else if (dataType.equals("Observation")) {
            Observation evalResource = (Observation) resource;
            System.out.println("Reference:" + evalResource.getSubject().getReference());
            code = evalResource.getCode();
            return checkCode(code, map);

        } else if (dataType.equals("Condition")) {
            Condition evalResource = (Condition) resource;
            System.out.println("Reference:" + evalResource.getSubject().getReference());
            code = evalResource.getCode();
            return checkCode(code, map);

        } else if (dataType.equals("ServiceRequest")) {
            ServiceRequest evalResource = (ServiceRequest) resource;
            System.out.println("Reference:" + evalResource.getSubject().getReference());
            code = evalResource.getCode();
            return checkCode(code, map);
        } else if (dataType.equals("DeviceRequest")) {
            DeviceRequest evalResource = (DeviceRequest) resource;
            System.out.println("Reference:" + evalResource.getSubject().getReference());
            code = evalResource.getCodeCodeableConcept();
            return checkCode(code, map);
        
        } else if (dataType.equals("Medication")) {
            Medication evalResource = (Medication) resource;
            code = evalResource.getCode();
            return checkCode(code, map);

        } else if (dataType.equals("MedicationAdministration")) {
            MedicationAdministration evalResource = (MedicationAdministration) resource;
            code = evalResource.getMedicationCodeableConcept();
            return checkCode(code, map);

        } else if (dataType.equals("Immunization")) {
            Immunization evalResource = (Immunization) resource;
            code = evalResource.getVaccineCode();
            return checkCode(code, map);

        } else if (dataType.equals("Coverage")) {
            Coverage evalResource = (Coverage) resource;
            System.out.println("Reference:" + evalResource.getPolicyHolder().getReference());
            code = evalResource.getType();
            return checkCode(code, map);
        }

        return validResource;
    }

    protected Collection<Object> executeQuery(String dataType, SearchParameterMap map) {

        List<IBaseResource> resourceList = new ArrayList<>();
        String searchURL = "/" + dataType + map.toNormalizedQueryString(myFhirContext);
        ourLog.info("The query string is " + searchURL);
        printMapKeys(map);
        if (patient_fhir.get().containsKey("dataBundle")) {
            IBaseBundle dataBundle = (IBaseBundle) patient_fhir.get().get("dataBundle");
            boolean resourceFound = false;
            if (dataBundle != null) {
                List<BundleEntryParts> parts = BundleUtil.toListOfEntries(myFhirContext, dataBundle);
                if (parts.size() > 0) {
                    System.out.println("Inside looking for datatype: " + dataType);
                    for (Iterator<BundleEntryParts> iterator = parts.iterator(); iterator.hasNext();) {
                        BundleEntryParts next = iterator.next();
                        IBaseResource resource = (IBaseResource) next.getResource();
                        String resourceType = resource.fhirType();
                        if (resourceType.equals(dataType)) {
                            resourceFound = true;
                            if (map.keySet().size()>1){
                                Boolean validResource = getValidResource(dataType, map, resource);
                                if (validResource) {
                                    resourceList.add(resource);
                                }
                            } else {
                                resourceList.add(resource);
                            }
                            

                        }

                    }

                }
            }
            if (!resourceFound) {
                System.out.println("*************Didn't find the resourceType:::" + dataType + "::::****************");
            }
        }

        Collection<Object> queryResult = resolveResourceList(resourceList);

        return queryResult;

    }

}