package org.opencds.cqf.r4.providers;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.hl7.fhir.r4.model.Attachment;
import org.hl7.fhir.r4.model.Binary;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Claim;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Composition;
import org.hl7.fhir.r4.model.DeviceRequest;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.Group;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.ListResource;
import org.hl7.fhir.r4.model.Measure;
import org.hl7.fhir.r4.model.MeasureReport;
import org.hl7.fhir.r4.model.Narrative;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.RelatedArtifact;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.ServiceRequest;
import org.hl7.fhir.r4.model.StringType;
import org.cqframework.cql.cql2elm.LibraryManager;
import org.cqframework.cql.cql2elm.ModelManager;
import org.cqframework.cql.elm.execution.Library;
import org.hl7.fhir.MeasureScoring;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.instance.model.api.IAnyResource;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.utilities.xhtml.XhtmlNode;
import org.opencds.cqf.common.evaluation.EvaluationProviderFactory;
import org.opencds.cqf.common.helpers.TranslatorHelper;
import org.opencds.cqf.common.providers.LibraryResolutionProvider;
import org.opencds.cqf.common.retrieve.InMemoryRetrieveProvider;
import org.opencds.cqf.common.retrieve.JpaFhirRetrieveProvider;
import org.opencds.cqf.common.retrieve.RemoteRetrieveProvider;
import org.opencds.cqf.cql.execution.Context;
import org.opencds.cqf.cql.execution.LibraryLoader;
import org.opencds.cqf.library.r4.NarrativeProvider;
import org.opencds.cqf.measure.r4.CqfMeasure;
import org.opencds.cqf.r4.evaluation.MeasureEvaluation;
import org.opencds.cqf.r4.evaluation.MeasureEvaluationSeed;
import org.opencds.cqf.r4.helpers.LibraryHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.jpa.dao.DaoRegistry;
import ca.uhn.fhir.jpa.dao.IFhirResourceDao;
import ca.uhn.fhir.jpa.rp.r4.LibraryResourceProvider;
import ca.uhn.fhir.jpa.rp.r4.MeasureResourceProvider;
import ca.uhn.fhir.jpa.searchparam.SearchParameterMap;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.annotation.OptionalParam;
import ca.uhn.fhir.rest.annotation.RequiredParam;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.api.RestOperationTypeEnum;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.param.ReferenceParam;
import ca.uhn.fhir.rest.param.TokenParam;
import ca.uhn.fhir.rest.param.TokenParamModifier;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;

public class MeasureOperationsProvider {

    private NarrativeProvider narrativeProvider;
    private HQMFProvider hqmfProvider;
    private DataRequirementsProvider dataRequirementsProvider;

    private LibraryResolutionProvider<org.hl7.fhir.r4.model.Library> libraryResolutionProvider;
    private MeasureResourceProvider measureResourceProvider;
    private DaoRegistry registry;
    private EvaluationProviderFactory factory;
    private LibraryResourceProvider defaultLibraryResourceProvider;
    

    private static final Logger logger = LoggerFactory.getLogger(MeasureOperationsProvider.class);

    public MeasureOperationsProvider(DaoRegistry registry, EvaluationProviderFactory factory,
            NarrativeProvider narrativeProvider, HQMFProvider hqmfProvider,
            LibraryResolutionProvider<org.hl7.fhir.r4.model.Library> libraryResolutionProvider,
            MeasureResourceProvider measureResourceProvider) {
        this.registry = registry;
        this.factory = factory;
        
        this.libraryResolutionProvider = libraryResolutionProvider;
        this.narrativeProvider = narrativeProvider;
        this.hqmfProvider = hqmfProvider;
        this.dataRequirementsProvider = new DataRequirementsProvider();
        this.measureResourceProvider = measureResourceProvider;
    }
    public MeasureOperationsProvider(DaoRegistry registry, EvaluationProviderFactory factory,
            NarrativeProvider narrativeProvider, HQMFProvider hqmfProvider,
            LibraryResourceProvider libraryResourceProvider,
            MeasureResourceProvider measureResourceProvider) {
        this(registry,factory,narrativeProvider,hqmfProvider, new LibraryOperationsProvider
        		(libraryResourceProvider, narrativeProvider),measureResourceProvider);
        this.defaultLibraryResourceProvider = libraryResourceProvider;
        
    }

    @Operation(name = "$hqmf", idempotent = true, type = Measure.class)
    public Parameters hqmf(@IdParam IdType theId) {
        Measure theResource = this.measureResourceProvider.getDao().read(theId);
        String hqmf = this.generateHQMF(theResource);
        Parameters p = new Parameters();
        p.addParameter().setValue(new StringType(hqmf));
        return p;
    }

    @Operation(name = "$refresh-generated-content", type = Measure.class)
    public MethodOutcome refreshGeneratedContent(HttpServletRequest theRequest, RequestDetails theRequestDetails,
            @IdParam IdType theId) {
        Measure theResource = this.measureResourceProvider.getDao().read(theId);

        theResource.getRelatedArtifact().removeIf(
                relatedArtifact -> relatedArtifact.getType().equals(RelatedArtifact.RelatedArtifactType.DEPENDSON));

        CqfMeasure cqfMeasure = this.dataRequirementsProvider.createCqfMeasure(theResource,
                this.libraryResolutionProvider);

        // Ensure All Related Artifacts for all referenced Libraries
        if (!cqfMeasure.getRelatedArtifact().isEmpty()) {
            for (RelatedArtifact relatedArtifact : cqfMeasure.getRelatedArtifact()) {
                boolean artifactExists = false;
                // logger.info("Related Artifact: " + relatedArtifact.getUrl());
                for (RelatedArtifact resourceArtifact : theResource.getRelatedArtifact()) {
                    if (resourceArtifact.equalsDeep(relatedArtifact)) {
                        // logger.info("Equals deep true");
                        artifactExists = true;
                        break;
                    }
                }
                if (!artifactExists) {
                    theResource.addRelatedArtifact(relatedArtifact.copy());
                }
            }
        }

        try {
            Narrative n = this.narrativeProvider.getNarrative(this.measureResourceProvider.getContext(), cqfMeasure);
            theResource.setText(n.copy());
        } catch (Exception e) {
            // Ignore the exception so the resource still gets updated
        }

        return this.measureResourceProvider.update(theRequest, theResource, theId,
                theRequestDetails.getConditionalUrl(RestOperationTypeEnum.UPDATE), theRequestDetails);
    }

    @Operation(name = "$get-narrative", idempotent = true, type = Measure.class)
    public Parameters getNarrative(@IdParam IdType theId) {
        Measure theResource = this.measureResourceProvider.getDao().read(theId);
        CqfMeasure cqfMeasure = this.dataRequirementsProvider.createCqfMeasure(theResource,
                this.libraryResolutionProvider);
        Narrative n = this.narrativeProvider.getNarrative(this.measureResourceProvider.getContext(), cqfMeasure);
        Parameters p = new Parameters();
        p.addParameter().setValue(new StringType(n.getDivAsString()));
        return p;
    }

    private String generateHQMF(Measure theResource) {
        CqfMeasure cqfMeasure = this.dataRequirementsProvider.createCqfMeasure(theResource,
                this.libraryResolutionProvider);
        return this.hqmfProvider.generateHQMF(cqfMeasure);
    }

    /*
     *
     * NOTE that the source, user, and pass parameters are not standard parameters
     * for the FHIR $evaluate-measure operation
     *
     */
    @Operation(name = "$evaluate-measure", idempotent = true, type = Measure.class)
    public MeasureReport evaluateMeasure(@IdParam IdType theId, @RequiredParam(name = "periodStart") String periodStart,
            @RequiredParam(name = "periodEnd") String periodEnd, @OptionalParam(name = "measure") String measureRef,
            @OptionalParam(name = "reportType") String reportType, @OptionalParam(name = "subject") String patientRef,
            @OptionalParam(name = "productLine") String productLine,
            @OptionalParam(name = "practitioner") String practitionerRef,
            @OptionalParam(name = "lastReceivedOn") String lastReceivedOn,
            @OptionalParam(name = "source") String source, @OptionalParam(name = "user") String user,
            @OptionalParam(name = "pass") String pass) throws InternalErrorException, FHIRException {

        // logger.info("Starting evaluate");
        LibraryLoader libraryLoader = LibraryHelper.createLibraryLoader(this.libraryResolutionProvider);
        // logger.info("library loading complete");
        MeasureEvaluationSeed seed = new MeasureEvaluationSeed(this.factory, libraryLoader,
                this.libraryResolutionProvider);
        // logger.info("get measure dao");
        Measure measure = this.measureResourceProvider.getDao().read(theId);
        // logger.info("measure dao available");
        if (measure == null) {
            throw new RuntimeException("Could not find Measure/" + theId.getIdPart());
        }

        seed.setup(measure, periodStart, periodEnd, productLine, source, user, pass);

        // resolve report type
        // logger.info("create evaluator");
        MeasureEvaluation evaluator = new MeasureEvaluation(seed.getDataProvider(), this.registry,
                seed.getMeasurementPeriod());
        // logger.info("evaluator ready");
        if (reportType != null) {
            switch (reportType) {
                case "patient":
                    return evaluator.evaluatePatientMeasure(seed.getMeasure(), seed.getContext(), patientRef);
                case "patient-list":
                    return evaluator.evaluateSubjectListMeasure(seed.getMeasure(), seed.getContext(), practitionerRef);
                case "population":
                    return evaluator.evaluatePopulationMeasure(seed.getMeasure(), seed.getContext());
                default:
                    throw new IllegalArgumentException("Invalid report type: " + reportType);
            }
        }

        // default report type is patient
        MeasureReport report = evaluator.evaluatePatientMeasure(seed.getMeasure(), seed.getContext(), patientRef);
        if (productLine != null) {
            Extension ext = new Extension();
            ext.setUrl("http://hl7.org/fhir/us/cqframework/cqfmeasures/StructureDefinition/cqfm-productLine");
            ext.setValue(new StringType(productLine));
            report.addExtension(ext);
        }
        // logger.info("Stopping evaluate");
        return report;
    }

    // @Operation(name = "$evaluate-measure-with-source", idempotent = true)
    // public MeasureReport evaluateMeasure(@IdParam IdType theId,
    // @OperationParam(name = "sourceData", min = 1, max = 1, type = Bundle.class)
    // Bundle sourceData,
    // @OperationParam(name = "periodStart", min = 1, max = 1) String periodStart,
    // @OperationParam(name = "periodEnd", min = 1, max = 1) String periodEnd) {
    // if (periodStart == null || periodEnd == null) {
    // throw new IllegalArgumentException("periodStart and periodEnd are required
    // for measure evaluation");
    // }
    // LibraryLoader libraryLoader =
    // LibraryHelper.createLibraryLoader(this.libraryResourceProvider);
    // MeasureEvaluationSeed seed = new MeasureEvaluationSeed(this.factory,
    // libraryLoader, this.libraryResourceProvider);
    // Measure measure = this.getDao().read(theId);

    // if (measure == null) {
    // throw new RuntimeException("Could not find Measure/" + theId.getIdPart());
    // }

    // seed.setup(measure, periodStart, periodEnd, null, null, null, null);
    // BundleDataProviderStu3 bundleProvider = new
    // BundleDataProviderStu3(sourceData);
    // bundleProvider.setTerminologyProvider(provider.getTerminologyProvider());
    // seed.getContext().registerDataProvider("http://hl7.org/fhir",
    // bundleProvider);
    // MeasureEvaluation evaluator = new MeasureEvaluation(bundleProvider,
    // seed.getMeasurementPeriod());
    // return evaluator.evaluatePatientMeasure(seed.getMeasure(), seed.getContext(),
    // "");
    // }

    /****
     * 
     * New operation being written to evaluate library with criteria
     * 
     */

    @Operation(name = "$lib-evaluate", idempotent = true, type = Measure.class)
    public Parameters libraryEvaluate(@OperationParam(name = "libraryId") String libraryId,
            @OperationParam(name = "criteria") String criteria, @OperationParam(name = "subject") String patientRef,
            @OperationParam(name = "periodStart") String periodStart,
            @OperationParam(name = "periodEnd") String periodEnd,
            @OperationParam(name = "source") String source,
            @OperationParam(name = "patientServerUrl") String patientServerUrl,
            @OperationParam(name = "patientServerToken") String patientServerToken,
            @OperationParam(name = "criteriaList") String criteriaList,
            @OperationParam(name = "valueSetsBundle", min = 1, max = 1, type = Bundle.class) Bundle valueSetsBundle,
            @OperationParam(name = "dataBundle", min = 1, max = 1, type = Bundle.class) Bundle dataBundle,
            @OperationParam(name = "libBundle", min = 1, max = 1, type = Bundle.class) Bundle libBundle,
            @OperationParam(name = "user") String user, @OperationParam(name = "parameters") Parameters parameters,
            @OperationParam(name = "pass") String pass) throws InternalErrorException, FHIRException {
        
        ArrayList<String> evalCriterias = new ArrayList<>();
        
        logger.info("in the library evaluate function");
        logger.info("library id: " + libraryId);
        logger.info("criteria:" + criteria);
        logger.info("criteriaList:" + criteriaList);
        String retrieverType = MeasureEvaluationSeed.LOCAL_RETRIEVER;
        // logger.info("periodStart:" + periodStart);
        // logger.info("patientRef:" + patientRef);
        // logger.info("patientServerUrl:" + patientServerUrl);
        // logger.info("patientServerToken:" + patientServerToken);
        // Setting server url and token for non local data access
        if (criteria == null && criteriaList == null){
            throw new RuntimeException("Either Criteria or Criteria List should be given");
        }
        if (criteria != null){
            evalCriterias.add(criteria);
        } else {
            String[] cList = criteriaList.split(",");
            for (String crit:cList){
                evalCriterias.add(crit);
            }
        }
        if (libraryId == null){
            throw new RuntimeException(" LibraryId cannot be null");
        }
        HashMap<String, Object> nonLocal = new HashMap<>();
        Boolean local = true;
        if (patientServerUrl != null) {
            if (patientServerUrl != "") {
                nonLocal.put("patient_server_url", patientServerUrl);
                local = false;
                retrieverType = MeasureEvaluationSeed.REMOTE_RETRIEVER;
            }

        }
        if (patientServerToken != null) {
            if (patientServerToken != "") {
                nonLocal.put("patient_server_token", patientServerToken);
            }

		}
		if (dataBundle != null) {
            retrieverType = MeasureEvaluationSeed.INMEMORY_RETRIEVER;
            nonLocal.put("dataBundle", dataBundle);
            InMemoryRetrieveProvider.patient_fhir.set(nonLocal);

        }
        if (!local) {
            RemoteRetrieveProvider.patient_fhir.set(nonLocal);
        }
        
        LibraryResourceProvider rp = new LibraryResourceProvider();
        rp.setDao(defaultLibraryResourceProvider.getDao());
        
        if(libBundle != null){
            rp.setDao(new LibraryBundleDao(libBundle));
            
        }
        LibraryOperationsProvider libOpsProvider = new LibraryOperationsProvider(rp, this.narrativeProvider);
        LibraryLoader libraryLoader = LibraryHelper.createLibraryLoader(libOpsProvider);
        MeasureEvaluationSeed seed = new MeasureEvaluationSeed(this.factory, libraryLoader,libOpsProvider);
        seed.setRetrieverType(retrieverType);
        if(valueSetsBundle !=null){
            seed.setValueSetsBundle(valueSetsBundle);
        }
        seed.setupLibrary(libraryId, periodStart, periodEnd,null,source, user, pass);
        MeasureEvaluation evaluator = new MeasureEvaluation(seed.getDataProvider(), 
                                            this.registry,seed.getMeasurementPeriod());
        Context context = null;
        Library library = null;

        context = seed.getContext();
        library = seed.getLibrary();
        if (parameters != null) {
            for (Parameters.ParametersParameterComponent pc : parameters.getParameter()) {
                context.setParameter(null, pc.getName(), pc.getResource());
            }
        }
        Parameters result = new Parameters();
        try {

            result = evaluator.cqlEvaluate(context, patientRef, evalCriterias, library);

        } catch (RuntimeException re) {
            re.printStackTrace();

            String message = re.getMessage() != null ? re.getMessage() : re.getClass().getName();
            result.addParameter().setName("error").setValue(new StringType(message));
        }
        return result;

    }

    @Operation(name = "$care-gaps", idempotent = true, type = Measure.class)
    public Parameters careGapsReport(@RequiredParam(name = "periodStart") String periodStart,
            @OptionalParam(name = "periodEnd") String periodEnd, @OptionalParam(name = "topic") String topic,
            @RequiredParam(name = "subject") String patientRef, @OptionalParam(name = "program") String program,
            @OperationParam(name = "measure") String measureparam,
            @OptionalParam(name = "practitioner") String practitionerRef,
            @OperationParam(name = "status") String status) {
        // logger.info("Caregap starting");
        Parameters parameters = new Parameters();

        List<Patient> patients = new ArrayList<>();
        List<Measure> measures = new ArrayList<>();

        if ((patientRef == null) & (practitionerRef == null)) {
            throw new RuntimeException("Subject and Practitioner both cannot be null!");
        }

        boolean topicgiven = false;
        boolean measuregiven = false;

        // This is temporary fix to get the topics for searching with texts
        Hashtable<String, String> topic_codes = new Hashtable<String, String>();
        topic_codes.put("75484-6", "Preventive Care");
        topic_codes.put("91393-9", "Management of Chronic Conditions");
        topic_codes.put("44943-9", "Prevention and Treatment of Opioid and Substance Use Disorders");

        if (topic != null) {
            boolean valueCodingSearch = false;
            String valueCode = "";
            SearchParameterMap topicSearch;
            // if topic in known code use codesearch else text search
            if (topic_codes.containsKey(topic)) {
                valueCodingSearch = false;
                valueCode = topic;
            }

            if (valueCodingSearch) {
                topicSearch = new SearchParameterMap().add("topic",
                        new TokenParam().setModifier(TokenParamModifier.valueOf("coding")).setValue(valueCode));
                // logger.info("code search : "+
                // topicSearch.toNormalizedQueryString(FhirContext.forR4()));
            } else {
                topicSearch = new SearchParameterMap().add("topic",
                        new TokenParam().setModifier(TokenParamModifier.TEXT).setValue(topic));
            }

            // logger.info("Topic search : "+
            // topicSearch.toNormalizedQueryString(FhirContext.forR4()));
            // add the search parametere to search for only activel measures

            topicSearch.add("status", new TokenParam().setValue("active"));
            // logger.info("status search : "+
            // topicSearch.toNormalizedQueryString(FhirContext.forR4()));

            IBundleProvider bundleProvider = this.measureResourceProvider.getDao().search(topicSearch);
            List<IBaseResource> resources = bundleProvider.getResources(0, 10000);
            for (Iterator iterator = resources.iterator(); iterator.hasNext();) {
                Measure res = (Measure) (iterator.next());
                measures.add(res);
            }
            logger.info("t: " + measures.size());

        }
        if (measureparam != null) {

            IBundleProvider bundleProvider = this.measureResourceProvider.getDao().search(new SearchParameterMap()
                    .add("_id", new TokenParam().setModifier(TokenParamModifier.TEXT).setValue(measureparam)));

            List<IBaseResource> resources = bundleProvider.getResources(0, 10000);
            for (Iterator iterator = resources.iterator(); iterator.hasNext();) {
                Measure res = (Measure) (iterator.next());
                measures.add(res);
            }
        }
        if (practitionerRef != null) {
            List<Patient> practitionerPatients = getPractitionerPatients(practitionerRef);
            patients.addAll(practitionerPatients);
        }
        if (patientRef != null) {
            patients.add(getPatients(patientRef));
        }
        logger.info("get library loader");
        LibraryLoader libraryLoader = LibraryHelper.createLibraryLoader(this.libraryResolutionProvider);
        logger.info("get seed");
        MeasureEvaluationSeed seed = new MeasureEvaluationSeed(this.factory, libraryLoader,
                this.libraryResolutionProvider);
        for (Iterator iterator = patients.iterator(); iterator.hasNext();) {
            Patient patient = (Patient) iterator.next();
            Bundle careGapBundle = getCareGapReport(patient, measures, status, periodStart, periodEnd, seed);
            parameters.addParameter(new Parameters.ParametersParameterComponent()
                    .setName("Care Gap Report for " + patient.getIdBase()).setResource(careGapBundle));
            // careGapReport.addEntry(new
            // Bundle.BundleEntryComponent().setResource(careGapBundle));

        }
        // logger.info("Leaving Caregap");
        return parameters;

    }

    private Bundle getCareGapReport(Patient patient, List<Measure> measures, String status, String periodStart,
            String periodEnd, MeasureEvaluationSeed seed) {
        Bundle careGapReport = new Bundle();
        careGapReport.setType(Bundle.BundleType.DOCUMENT);

        Composition composition = new Composition();
        // TODO - this is a placeholder code for now ... replace with preferred code
        // once identified
        CodeableConcept typeCode = new CodeableConcept()
                .addCoding(new Coding().setSystem("http://loinc.org").setCode("57024-2"));
        composition.setStatus(Composition.CompositionStatus.FINAL).setType(typeCode);

        composition.setSubject(new Reference(patient)).setTitle("Care Gap Report for Patient:" + patient.getId());
        List<MeasureReport> reports = new ArrayList<>();
        MeasureReport report = new MeasureReport();
        for (Measure measure : measures) {
            logger.info("measure:" + measure.getId());
            Composition.SectionComponent section = new Composition.SectionComponent();
            section.addEntry(new Reference(measure));
            if (measure.hasTitle()) {
                section.setTitle(measure.getTitle());
            }
            CodeableConcept improvementNotation = new CodeableConcept().addCoding(new Coding().setCode("increase")
                    .setSystem("http://terminology.hl7.org/CodeSystem/measure-improvement-notation")); // defaulting to
                                                                                                       // "increase"
            if (measure.hasImprovementNotation()) {
                improvementNotation = measure.getImprovementNotation();
                section.setText(new Narrative().setStatus(Narrative.NarrativeStatus.GENERATED)
                        .setDiv(new XhtmlNode().setValue(improvementNotation.getCodingFirstRep().getCode())));
            }

            seed.setup(measure, periodStart, periodEnd, null, null, null, null);
            MeasureEvaluation evaluator = new MeasureEvaluation(seed.getDataProvider(), this.registry,
                    seed.getMeasurementPeriod());
            logger.info("The patient: " + patient.getId());
            report = evaluator.evaluatePatientMeasure(seed.getMeasure(), seed.getContext(), patient.getId());
            reports.add(report);
            composition.addSection(section);
        }

        careGapReport.addEntry(new Bundle.BundleEntryComponent().setResource(composition));
        logger.info("The number of reports are " + reports.size());
        // Add the reports based on status parameter
        boolean addreport;
        for (MeasureReport rep : reports) {
            for (MeasureReport.MeasureReportGroupComponent group : rep.getGroup()) {
                addreport = false;
                if (group.hasMeasureScore()) {
                    BigDecimal scorevalue = group.getMeasureScore().getValue();
                    logger.info("the measure score is " + scorevalue.toString());
                    logger.info("the status is " + status);
                    if (status == null) {
                        addreport = true;
                    } else if ((scorevalue.compareTo(new BigDecimal(1)) == 0) && status.equals("closed-gaps")) {
                        addreport = true;
                    } else if ((scorevalue.compareTo(new BigDecimal(0)) == 0) && status.equals("open-gaps")) {
                        addreport = true;

                    } else {
                        addreport = false;
                    }
                }
                if (addreport) {
                    careGapReport.addEntry(new Bundle.BundleEntryComponent().setResource(rep));
                }
            }
        }
        return careGapReport;

    }

    private List<Patient> getGroupPatients(String groupId) {
        List<Patient> patients = new ArrayList<>();

        // // IIdType(GroupId));
        // patientgroup.getMember();
        return patients;
    }

    private List<Patient> getPractitionerPatients(String practitionerRef) {
        SearchParameterMap map = new SearchParameterMap();
        map.add("general-practitioner", new ReferenceParam(
                practitionerRef.startsWith("Practitioner/") ? practitionerRef : "Practitioner/" + practitionerRef));

        List<Patient> patients = new ArrayList<>();
        IBundleProvider patientProvider = registry.getResourceDao("Patient").search(map);
        List<IBaseResource> patientList = patientProvider.getResources(0, patientProvider.size());
        patientList.forEach(x -> patients.add((Patient) x));
        return patients;
    }

    private Patient getPatients(String patientRef) {
        if (!patientRef.startsWith("Patient/")) {
            patientRef = "Patient/" + patientRef;
        }
        IIdType id = new Reference(patientRef).getReferenceElement();
        Patient patient = (Patient) registry.getResourceDao("Patient").read(id);
        return patient;
    }

    @Operation(name = "$collect-data", idempotent = true, type = Measure.class)
    public Parameters collectData(@IdParam IdType theId, @RequiredParam(name = "periodStart") String periodStart,
            @RequiredParam(name = "periodEnd") String periodEnd, @OptionalParam(name = "subject") String patientRef,
            @OptionalParam(name = "practitioner") String practitionerRef,
            @OptionalParam(name = "lastReceivedOn") String lastReceivedOn) throws FHIRException {
        // TODO: Spec says that the periods are not required, but I am not sure what to
        // do when they aren't supplied so I made them required
        MeasureReport report = evaluateMeasure(theId, periodStart, periodEnd, null, null, patientRef, null,
                practitionerRef, lastReceivedOn, null, null, null);
        report.setGroup(null);

        Parameters parameters = new Parameters();

        parameters.addParameter(
                new Parameters.ParametersParameterComponent().setName("measureReport").setResource(report));

        if (report.hasContained()) {
            for (Resource contained : report.getContained()) {
                if (contained instanceof Bundle) {
                    addEvaluatedResourcesToParameters((Bundle) contained, parameters);
                }
            }
        }

        // TODO: need a way to resolve referenced resources within the evaluated
        // resources
        // Should be able to use _include search with * wildcard, but HAPI doesn't
        // support that

        return parameters;
    }

    private void addEvaluatedResourcesToParameters(Bundle contained, Parameters parameters) {
        Map<String, Resource> resourceMap = new HashMap<>();
        if (contained.hasEntry()) {
            for (Bundle.BundleEntryComponent entry : contained.getEntry()) {
                if (entry.hasResource() && !(entry.getResource() instanceof ListResource)) {
                    if (!resourceMap.containsKey(entry.getResource().getIdElement().getValue())) {
                        parameters.addParameter(new Parameters.ParametersParameterComponent().setName("resource")
                                .setResource(entry.getResource()));

                        resourceMap.put(entry.getResource().getIdElement().getValue(), entry.getResource());

                        resolveReferences(entry.getResource(), parameters, resourceMap);
                    }
                }
            }
        }
    }

    private void resolveReferences(Resource resource, Parameters parameters, Map<String, Resource> resourceMap) {
        List<IBase> values;
        for (BaseRuntimeChildDefinition child : this.measureResourceProvider.getContext()
                .getResourceDefinition(resource).getChildren()) {
            values = child.getAccessor().getValues(resource);
            if (values == null || values.isEmpty()) {
                continue;
            }

            else if (values.get(0) instanceof Reference
                    && ((Reference) values.get(0)).getReferenceElement().hasResourceType()
                    && ((Reference) values.get(0)).getReferenceElement().hasIdPart()) {
                Resource fetchedResource = (Resource) registry
                        .getResourceDao(((Reference) values.get(0)).getReferenceElement().getResourceType())
                        .read(new IdType(((Reference) values.get(0)).getReferenceElement().getIdPart()));

                if (!resourceMap.containsKey(fetchedResource.getIdElement().getValue())) {
                    parameters.addParameter(new Parameters.ParametersParameterComponent().setName("resource")
                            .setResource(fetchedResource));

                    resourceMap.put(fetchedResource.getIdElement().getValue(), fetchedResource);
                }
            }
        }
    }

    // TODO - this needs a lot of work
    @Operation(name = "$data-requirements", idempotent = true, type = Measure.class)
    public org.hl7.fhir.r4.model.Library dataRequirements(@IdParam IdType theId,
            @RequiredParam(name = "startPeriod") String startPeriod,
            @RequiredParam(name = "endPeriod") String endPeriod) throws InternalErrorException, FHIRException {

        Measure measure = this.measureResourceProvider.getDao().read(theId);
        return this.dataRequirementsProvider.getDataRequirements(measure, this.libraryResolutionProvider);
    }

    @Operation(name = "$submit-data", idempotent = true, type = Measure.class)
    public Resource submitData(RequestDetails details, @IdParam IdType theId,
            @OperationParam(name = "measureReport", min = 1, max = 1, type = MeasureReport.class) MeasureReport report,
            @OperationParam(name = "resource") List<IAnyResource> resources) {
        Bundle transactionBundle = new Bundle().setType(Bundle.BundleType.TRANSACTION);

        /*
         * TODO - resource validation using $data-requirements operation (params are the
         * provided id and the measurement period from the MeasureReport)
         * 
         * TODO - profile validation ... not sure how that would work ... (get
         * StructureDefinition from URL or must it be stored in Ruler?)
         */

        transactionBundle.addEntry(createTransactionEntry(report));

        for (IAnyResource resource : resources) {
            Resource res = (Resource) resource;
            if (res instanceof Bundle) {
                for (Bundle.BundleEntryComponent entry : createTransactionBundle((Bundle) res).getEntry()) {
                    transactionBundle.addEntry(entry);
                }
            } else {
                // Build transaction bundle
                transactionBundle.addEntry(createTransactionEntry(res));
            }
        }

        return (Resource) this.registry.getSystemDao().transaction(details, transactionBundle);
    }

    private Bundle createTransactionBundle(Bundle bundle) {
        Bundle transactionBundle;
        if (bundle != null) {
            if (bundle.hasType() && bundle.getType() == Bundle.BundleType.TRANSACTION) {
                transactionBundle = bundle;
            } else {
                transactionBundle = new Bundle().setType(Bundle.BundleType.TRANSACTION);
                if (bundle.hasEntry()) {
                    for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
                        if (entry.hasResource()) {
                            transactionBundle.addEntry(createTransactionEntry(entry.getResource()));
                        }
                    }
                }
            }
        } else {
            transactionBundle = new Bundle().setType(Bundle.BundleType.TRANSACTION).setEntry(new ArrayList<>());
        }

        return transactionBundle;
    }

    private Bundle.BundleEntryComponent createTransactionEntry(Resource resource) {
        Bundle.BundleEntryComponent transactionEntry = new Bundle.BundleEntryComponent().setResource(resource);
        if (resource.hasId()) {
            transactionEntry.setRequest(
                    new Bundle.BundleEntryRequestComponent().setMethod(Bundle.HTTPVerb.PUT).setUrl(resource.getId()));
        } else {
            transactionEntry.setRequest(new Bundle.BundleEntryRequestComponent().setMethod(Bundle.HTTPVerb.POST)
                    .setUrl(resource.fhirType()));
        }
        return transactionEntry;
    }

    
}
