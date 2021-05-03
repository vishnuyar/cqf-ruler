package org.opencds.cqf.r4.providers;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.CodeSystem;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.ValueSet;
import org.hl7.fhir.r4.model.ValueSet.ValueSetExpansionContainsComponent;
import org.opencds.cqf.cql.engine.runtime.Code;
import org.opencds.cqf.cql.engine.terminology.CodeSystemInfo;
import org.opencds.cqf.cql.engine.terminology.TerminologyProvider;
import org.opencds.cqf.cql.engine.terminology.ValueSetInfo;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.support.ValueSetExpansionOptions;
import ca.uhn.fhir.jpa.rp.r4.ValueSetResourceProvider;
import ca.uhn.fhir.jpa.term.api.ITermReadSvcR4;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.util.FhirVersionIndependentConcept;

public class JpaTerminologyProvider implements TerminologyProvider {

    private ITermReadSvcR4 terminologySvcR4;
    private FhirContext context;
    private ValueSetResourceProvider valueSetResourceProvider;
    private Bundle valueSetsBundle;

    public JpaTerminologyProvider(ITermReadSvcR4 terminologySvcR4, FhirContext context,
            ValueSetResourceProvider valueSetResourceProvider) {
        this.terminologySvcR4 = terminologySvcR4;
        this.context = context;
        this.valueSetsBundle = new Bundle();
        this.valueSetResourceProvider = valueSetResourceProvider;
    }

    @Override
    public synchronized boolean in(Code code, ValueSetInfo valueSet) throws ResourceNotFoundException {
        for (Code c : expand(valueSet)) {
            if (c == null)
                continue;
            if (c.getCode().equals(code.getCode()) && c.getSystem().equals(code.getSystem())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public synchronized Iterable<Code> expand(ValueSetInfo valueSet) throws ResourceNotFoundException {
        List<Code> codes = new ArrayList<>();
        boolean needsExpand = false;
        ValueSet vs = null;
        List<IBaseResource> valueSets = new ArrayList<>();
        //System.out.println("Looking for valueset: " + valueSet.getId());
        if (valueSetsBundle.getEntry().size() > 1) {
            for (Bundle.BundleEntryComponent entry : valueSetsBundle.getEntry()) {
                if (entry.getResource().getResourceType().toString().equals("ValueSet")) {
                    if (entry.getResource().getIdElement().getIdPart().equals(valueSet.getId())) {
                        valueSets.add(entry.getResource());
                    }

                }
            }

        } else if (valueSet.getId().startsWith("http://") || valueSet.getId().startsWith("https://")) {
            //System.out.println("looking for valueset from Remote");
            ValueSet remoteValueSet = getRemoteValueSet(valueSet.getId());
            valueSets.add(remoteValueSet);
            System.out.println("verison: " + valueSet.getVersion());
            System.out.println("codesystem size: " + valueSet.getCodeSystems().size());
            if (valueSet.getVersion() != null
                    || (valueSet.getCodeSystems() != null && valueSet.getCodeSystems().size() > 0)) {
                if (!(valueSet.getCodeSystems().size() == 1 && valueSet.getCodeSystems().get(0).getVersion() == null)) {
                    throw new UnsupportedOperationException(String.format(
                            "Could not expand value set %s; version and code system bindings are not supported at this time.",
                            valueSet.getId()));
                }
            }
            System.out.println("size of sets: " + valueSets.size());
            // IBundleProvider bundleProvider = valueSetResourceProvider.getDao().search(new
            // SearchParameterMap().add(ValueSet.SP_URL, new UriParam(valueSet.getId())));
            // valueSets = bundleProvider.getResources(0, bundleProvider.size());

        } else {
            vs = valueSetResourceProvider.getDao().read(new IdType(valueSet.getId()));
        }

        if (valueSets.size() == 1) {
            vs = (ValueSet) valueSets.get(0);
        }
        if (valueSets.isEmpty() && vs == null) {
            throw new IllegalArgumentException(String.format("Could not resolve value set %s.", valueSet.getId()));
        } else if (valueSets.size() > 1) {
            throw new IllegalArgumentException("Found more than 1 ValueSet with url: " + valueSet.getId());
        }
        if (vs != null) {
            if (vs.hasCompose()) {
                if (vs.getCompose().hasInclude()) {
                    for (ValueSet.ConceptSetComponent include : vs.getCompose().getInclude()) {
                        if (include.hasValueSet() || include.hasFilter()) {
                            needsExpand = true;
                            break;
                        }
                        for (ValueSet.ConceptReferenceComponent concept : include.getConcept()) {
                            if (concept.hasCode()) {
                                codes.add(new Code().withCode(concept.getCode()).withSystem(include.getSystem()));
                            }
                        }
                    }
                    if (!needsExpand) {
                        return codes;
                    }
                }
            }

            if (vs.hasExpansion() && vs.getExpansion().hasContains()) {
                for (ValueSetExpansionContainsComponent vsecc : vs.getExpansion().getContains()) {
                    codes.add(new Code().withCode(vsecc.getCode()).withSystem(vsecc.getSystem()));
                }

                return codes;
            }

        }

        List<FhirVersionIndependentConcept> expansion = terminologySvcR4.expandValueSet(new ValueSetExpansionOptions(),
                valueSet.getId());
        for (FhirVersionIndependentConcept concept : expansion) {
            codes.add(new Code().withCode(concept.getCode()).withSystem(concept.getSystem()));
        }

        return codes;
    }

    private ValueSet getRemoteValueSet(String valueSetUrl) {
        try {
            System.out.println("the baseurl is " + valueSetUrl);
            // Hardcoding for NLM Valueset. If the url contains nlm, check the valueset
            // already available internally
            if (valueSetUrl.contains("nlm")) {
                String nlmId = valueSetUrl.substring(valueSetUrl.lastIndexOf('/') + 1);
                //System.out.println("Looking for nlm id " + nlmId);
                ValueSet nlmvs = valueSetResourceProvider.getDao().read(new IdType(nlmId));
                return nlmvs;
            } else {
                // Adding the format parameter to get in json format
                URL url = new URL(valueSetUrl + "?_format=json");
                StringBuilder sb = new StringBuilder();
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                if (conn.getResponseCode() < HttpURLConnection.HTTP_BAD_REQUEST) {
                    BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
                    String line = null;
                    while ((line = in.readLine()) != null) {
                        sb.append(line);
                    }
                    String result = sb.toString();
                    System.out.println(result);
                    IParser parser = FhirContext.forR4().newJsonParser();
                    ValueSet vs = parser.parseResource(ValueSet.class, result);
                    System.out.println(FhirContext.forR4().newJsonParser().encodeResourceToString(vs));
                    return vs;
                } else {
                    System.out.println(conn.getResponseCode());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();

        }

        return null;
    }

    @Override
    public synchronized Code lookup(Code code, CodeSystemInfo codeSystem) throws ResourceNotFoundException {
        CodeSystem cs = (CodeSystem) terminologySvcR4.fetchCodeSystem(codeSystem.getId());
        for (CodeSystem.ConceptDefinitionComponent concept : cs.getConcept()) {
            if (concept.getCode().equals(code.getCode()))
                return code.withSystem(codeSystem.getId()).withDisplay(concept.getDisplay());
        }
        return code;
    }

    public Bundle getValueSetsBundle() {
        return valueSetsBundle;
    }

    public void setValueSetsBundle(Bundle valueSetsBundle) {
        this.valueSetsBundle = valueSetsBundle;
    }
}
