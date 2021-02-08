package org.opencds.cqf.r4.providers;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import org.hl7.fhir.r4.model.Attachment;
import org.hl7.fhir.r4.model.BaseReference;
import org.hl7.fhir.r4.model.Binary;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Claim;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Composition;
import org.hl7.fhir.r4.model.DetectedIssue;
import org.hl7.fhir.r4.model.DeviceRequest;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.Group;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.ListResource;
import org.hl7.fhir.r4.model.Measure;
import org.hl7.fhir.r4.model.MeasureReport;
import org.hl7.fhir.r4.model.Meta;
import org.hl7.fhir.r4.model.Narrative;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.RelatedArtifact;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.ServiceRequest;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.Group.GroupMemberComponent;
import org.hl7.fhir.r4.model.ListResource.ListEntryComponent;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;
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
import org.opencds.cqf.cql.engine.execution.Context;
import org.opencds.cqf.common.evaluation.LibraryLoader;
import org.opencds.cqf.library.r4.NarrativeProvider;
import org.opencds.cqf.cql.engine.data.DataProvider;
import org.opencds.cqf.measure.r4.CqfMeasure;
import org.opencds.cqf.r4.evaluation.MeasureEvaluation;
import org.opencds.cqf.r4.evaluation.MeasureEvaluationSeed;
import org.opencds.cqf.r4.helpers.LibraryHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.jpa.api.dao.DaoRegistry;
import ca.uhn.fhir.jpa.api.dao.IFhirResourceDao;
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
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;

public class MeasureOperationsProvider {

    private NarrativeProvider narrativeProvider;
    private HQMFProvider hqmfProvider;
    private DataRequirementsProvider dataRequirementsProvider;

    private LibraryResolutionProvider<org.hl7.fhir.r4.model.Library> libraryResolutionProvider;
    private MeasureResourceProvider measureResourceProvider;
    private DaoRegistry registry;
    private EvaluationProviderFactory factory;
    private LibraryResourceProvider defaultLibraryResourceProvider;
    private HashMap<String, Object> nonLocal;
    private Boolean local;
    private String retrieverType;

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
        this.nonLocal = new HashMap<>();
        this.local = true;
        this.retrieverType = MeasureEvaluationSeed.LOCAL_RETRIEVER;
    }

    public MeasureOperationsProvider(DaoRegistry registry, EvaluationProviderFactory factory,
            NarrativeProvider narrativeProvider, HQMFProvider hqmfProvider,
            LibraryResourceProvider libraryResourceProvider, MeasureResourceProvider measureResourceProvider) {
        this(registry, factory, narrativeProvider, hqmfProvider,
                new LibraryOperationsProvider(libraryResourceProvider, narrativeProvider), measureResourceProvider);
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
    public MeasureReport evaluateMeasure(@IdParam IdType theId,
            @OperationParam(name = "periodStart") String periodStart,
            @OperationParam(name = "periodEnd") String periodEnd, @OperationParam(name = "measure") String measureRef,
            @OperationParam(name = "reportType") String reportType, @OperationParam(name = "subject") String patientRef,
            @OperationParam(name = "productLine") String productLine,
            @OperationParam(name = "practitioner") String practitionerRef,
            @OperationParam(name = "lastReceivedOn") String lastReceivedOn,
            @OperationParam(name = "patientServerUrl") String patientServerUrl,
            @OperationParam(name = "patientServerToken") String patientServerToken,
            @OperationParam(name = "dataBundle", min = 1, max = 1, type = Bundle.class) Bundle dataBundle,
            @OperationParam(name = "source") String source, @OperationParam(name = "user") String user,
            @OperationParam(name = "pass") String pass) throws InternalErrorException, FHIRException {

        this.retrieverType = MeasureEvaluationSeed.LOCAL_RETRIEVER;
        LibraryLoader libraryLoader = LibraryHelper.createLibraryLoader(this.libraryResolutionProvider);
        MeasureEvaluationSeed seed = new MeasureEvaluationSeed(this.factory, libraryLoader,
                this.libraryResolutionProvider);
        // logger.info("get measure dao");
        Measure measure = this.measureResourceProvider.getDao().read(theId);
        logger.info("patientserverurl: " + patientServerUrl);
        if (measure == null) {
            throw new RuntimeException("Could not find Measure/" + theId.getIdPart());
        }
        if (patientServerUrl != null) {
            if (patientServerUrl != "") {
                nonLocal.put("patient_server_url", patientServerUrl);
                this.local = false;
                this.retrieverType = MeasureEvaluationSeed.REMOTE_RETRIEVER;
            }

        }
        if (patientServerToken != null) {
            if (patientServerToken != "") {
                this.nonLocal.put("patient_server_token", patientServerToken);
            }

        }
        if (dataBundle != null) {
            this.retrieverType = MeasureEvaluationSeed.INMEMORY_RETRIEVER;
            this.nonLocal.put("dataBundle", dataBundle);
            InMemoryRetrieveProvider.patient_fhir.set(nonLocal);

        }
        if (!local) {
            RemoteRetrieveProvider.patient_fhir.set(this.nonLocal);
        }
        seed.setRetrieverType(this.retrieverType);
        seed.setup(measure, periodStart, periodEnd, productLine, source, user, pass);
        if ((patientRef != null) && (patientRef.startsWith("Patient/"))) {
            patientRef = patientRef.replace("Patient/", "");
        }
        MeasureEvaluation evaluator = new MeasureEvaluation(seed.getDataProvider(), this.registry,
                seed.getMeasurementPeriod());
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

    @Operation(name = "$lib-evaluate", idempotent = true, type = Measure.class)
    public Parameters libraryEvaluate(@OperationParam(name = "libraryId") String libraryId,
            @OperationParam(name = "criteria") String criteria, @OperationParam(name = "subject") String patientRef,
            @OperationParam(name = "periodStart") String periodStart,
            @OperationParam(name = "periodEnd") String periodEnd, @OperationParam(name = "source") String source,
            @OperationParam(name = "patientServerUrl") String patientServerUrl,
            @OperationParam(name = "patientServerToken") String patientServerToken,
            @OperationParam(name = "libraryServerUrl") String libraryServerUrl,
            @OperationParam(name = "libraryServerToken") String libraryServerToken,
            @OperationParam(name = "criteriaList") String criteriaList,
            @OperationParam(name = "valueSetsBundle", min = 1, max = 1, type = Bundle.class) Bundle valueSetsBundle,
            @OperationParam(name = "dataBundle", min = 1, max = 1, type = Bundle.class) Bundle dataBundle,
            @OperationParam(name = "libBundle", min = 1, max = 1, type = Bundle.class) Bundle libBundle,
            @OperationParam(name = "user") String user, @OperationParam(name = "parameters") Parameters parameters,
            @OperationParam(name = "pass") String pass) throws InternalErrorException, FHIRException {

        ArrayList<String> evalCriterias = new ArrayList<>();
        this.retrieverType = MeasureEvaluationSeed.LOCAL_RETRIEVER;
        if (criteria == null && criteriaList == null) {
            throw new RuntimeException("Either Criteria or Criteria List should be given");
        }
        if (criteria != null) {
            evalCriterias.add(criteria);
        } else {
            String[] cList = criteriaList.split(",");
            for (String crit : cList) {
                evalCriterias.add(crit);
            }
        }
        if (libraryId == null) {
            throw new RuntimeException(" LibraryId cannot be null");
        }
        if (patientServerUrl != null) {
            if (patientServerUrl != "") {
                this.nonLocal.put("patient_server_url", patientServerUrl);
                this.local = false;
                this.retrieverType = MeasureEvaluationSeed.REMOTE_RETRIEVER;
            }
            if (patientServerToken != null) {
                if (patientServerToken != "") {
                    this.nonLocal.put("patient_server_token", patientServerToken);
                }

            }

        }

        if (dataBundle != null) {
            this.retrieverType = MeasureEvaluationSeed.INMEMORY_RETRIEVER;
            nonLocal.put("dataBundle", dataBundle);
            InMemoryRetrieveProvider.patient_fhir.set(nonLocal);

        }
        if (!local) {
            RemoteRetrieveProvider.patient_fhir.set(this.nonLocal);
        }

        LibraryResourceProvider rp = new LibraryResourceProvider();
        rp.setDao(defaultLibraryResourceProvider.getDao());

        if ((libraryServerUrl != null) && (libraryServerUrl != "")) {
            if ((libraryServerToken != null) && (libraryServerToken != "")) {
                rp.setDao(new RemoteLibraryBundleDao(libraryServerUrl,libraryServerToken));
            } else {
                rp.setDao(new RemoteLibraryBundleDao(libraryServerUrl,null));
            }
        }

        if (libBundle != null) {
            rp.setDao(new LibraryBundleDao(libBundle));

        }
        LibraryOperationsProvider libOpsProvider = new LibraryOperationsProvider(rp, this.narrativeProvider);
        LibraryLoader libraryLoader = LibraryHelper.createLibraryLoader(libOpsProvider);
        MeasureEvaluationSeed seed = new MeasureEvaluationSeed(this.factory, libraryLoader, libOpsProvider);
        seed.setRetrieverType(this.retrieverType);
        if (valueSetsBundle != null) {
            seed.setValueSetsBundle(valueSetsBundle);
        }
        seed.setupLibrary(libraryId, periodStart, periodEnd, null, source, user, pass);
        MeasureEvaluation evaluator = new MeasureEvaluation(seed.getDataProvider(), this.registry,
                seed.getMeasurementPeriod());
        Context context = null;
        Library library = null;

        context = seed.getContext();
        library = seed.getLibrary();
        if (parameters != null) {
            for (Parameters.ParametersParameterComponent pc : parameters.getParameter()) {
                if (pc.getResource() != null){
                    context.setParameter(null, pc.getName(), pc.getResource());
                }
                else if (pc.getValue() != null){
                    System.out.println("The value parameter value is :"+pc.getValue());
                    if (pc.getValue() instanceof BooleanType){
                        boolean val = ((BooleanType) pc.getValue()).booleanValue();
                        context.setParameter(null, pc.getName(), val);
                    } 
                    
                }
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
    public Resource careGapsReport(@OperationParam(name = "periodStart") String periodStart,
            @OperationParam(name = "periodEnd") String periodEnd, @OperationParam(name = "topic") String topic,
            @OperationParam(name = "subject") String patientRef, @OperationParam(name = "program") String program,
            @OperationParam(name = "measure") String measureparam,
            @OperationParam(name = "practitioner") String practitionerRef,
            @OperationParam(name = "patientServerUrl") String patientServerUrl,
            @OperationParam(name = "patientServerToken") String patientServerToken,
            @OperationParam(name = "dataBundle", min = 1, max = 1, type = Bundle.class) Bundle dataBundle,
            @OperationParam(name = "status") String status) {

        Parameters parameters = new Parameters();
        this.retrieverType = MeasureEvaluationSeed.LOCAL_RETRIEVER;
        List<String> patients = new ArrayList<>();
        List<Measure> measures = new ArrayList<>();
        boolean patientSearch = false;
        boolean groupSearch = false;

        if ((patientRef == null) & (practitionerRef == null)) {
            throw new RuntimeException("Subject and Practitioner both cannot be null!");
        }

        if (patientRef != null) {
            if (patientRef.startsWith("Patient/")) {
                patientSearch = true;
            } else if (patientRef.startsWith("Group/")) {
                groupSearch = true;
            } else {
                throw new RuntimeException("Subject should start with either Patient or Group");
            }
        }

        System.out.println("Patient server url: " + patientServerUrl);
        boolean topicgiven = false;
        boolean measuregiven = false;
        if (patientServerUrl != null) {
            if (patientServerUrl != "") {
                nonLocal.put("patient_server_url", patientServerUrl);
                this.local = false;
                this.retrieverType = MeasureEvaluationSeed.REMOTE_RETRIEVER;
            }

        }
        if (patientServerToken != null) {
            if (patientServerToken != "") {
                this.nonLocal.put("patient_server_token", patientServerToken);
            }

        }
        if (dataBundle != null) {
            this.retrieverType = MeasureEvaluationSeed.INMEMORY_RETRIEVER;
            this.nonLocal.put("dataBundle", dataBundle);
            InMemoryRetrieveProvider.patient_fhir.set(nonLocal);

        }
        if (!local) {
            RemoteRetrieveProvider.patient_fhir.set(this.nonLocal);
        }

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
            topicSearch.add("status", new TokenParam().setValue("active"));
            IBundleProvider bundleProvider = this.measureResourceProvider.getDao().search(topicSearch);
            List<IBaseResource> resources = bundleProvider.getResources(0, 10000);
            for (Iterator iterator = resources.iterator(); iterator.hasNext();) {
                Measure res = (Measure) (iterator.next());
                measures.add(res);
            }

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

        if (measures.size() == 0) {
            throw new RuntimeException(
                    "Neither topic nor measure parameter resolves to available Measure for running care gap report");
        }

        LibraryLoader libraryLoader = LibraryHelper.createLibraryLoader(this.libraryResolutionProvider);
        MeasureEvaluationSeed seed = new MeasureEvaluationSeed(this.factory, libraryLoader,
                this.libraryResolutionProvider);
        seed.setRetrieverType(this.retrieverType);
        // Hack solution for getting Provider -- Needed for getting patients data
        seed.setup(measures.get(0), periodStart, periodEnd, null, null, null, null);
        if (practitionerRef != null) {
            List<String> practitionerPatients = getPractitionerPatients(practitionerRef, seed.getDataProvider());
            patients.addAll(practitionerPatients);
        }
        if (patientRef != null) {
            if (patientSearch) {
                patients.addAll(getPatients(patientRef, seed.getDataProvider()));
            }
            if (groupSearch) {
                patients.addAll(getGroupPatients(patientRef, seed.getDataProvider()));
            }

        }

        if (patients.size() > 1) {
            for (String patient : patients) {
                Bundle careGapBundle = getCareGapReport(patient, measures, status, periodStart, periodEnd, seed);
                parameters.addParameter(
                        new Parameters.ParametersParameterComponent().setName("return").setResource(careGapBundle));

            }
            return parameters;
        } else if (patients.size() == 1) {
            return getCareGapReport(patients.get(0), measures, status, periodStart, periodEnd, seed);

        } else {
            throw new RuntimeException("Subject not found for running care gap report");
        }

    }

    private Bundle getCareGapReport(String patientRef, List<Measure> measures, String status, String periodStart,
            String periodEnd, MeasureEvaluationSeed seed) {
        Bundle careGapReport = new Bundle();
        careGapReport.setType(Bundle.BundleType.DOCUMENT);
        careGapReport.setTimestamp(new Date());

        Composition composition = new Composition();
        if (patientRef.startsWith("Patient/")) {
            patientRef = patientRef.replace("Patient/", "");
        }
        CodeableConcept typeCode = new CodeableConcept();
        typeCode.addCoding(new Coding().setCode("gaps-doc")
                .setSystem("http://hl7.org/fhir/us/davinci-deqm/CodeSystem/gaps-doc-type")
                .setDisplay("Gaps in Care Report"));

        composition.setStatus(Composition.CompositionStatus.FINAL).setType(typeCode);

        composition.setSubject(new Reference("Patient/" + patientRef))
                .setTitle("Care Gap Report for Patient:" + patientRef);
        List<MeasureReport> reports = new ArrayList<>();
        List<DetectedIssue> detectedIssues = new ArrayList<DetectedIssue>();
        MeasureReport report = new MeasureReport();
        for (Measure measure : measures) {
            Composition.SectionComponent section = new Composition.SectionComponent();
            if (measure.hasTitle()) {
                section.setTitle(measure.getTitle());
            }

            seed.setup(measure, periodStart, periodEnd, null, null, null, null);
            MeasureEvaluation evaluator = new MeasureEvaluation(seed.getDataProvider(), this.registry,
                    seed.getMeasurementPeriod());
            report = evaluator.evaluatePatientMeasure(seed.getMeasure(), seed.getContext(), patientRef);
            report.setId(UUID.randomUUID().toString());
            report.setDate(new Date());
            report.setMeta(new Meta()
                    .addProfile("http://hl7.org/fhir/us/davinci-deqm/StructureDefinition/indv-measurereport-deqm"));
            section.setFocus(new Reference("MeasureReport/" + report.getId()));
            // Check for detected issue
            DetectedIssue detected = checkDetectedIssue(report, patientRef);
            if (detected != null) {
                detectedIssues.add(detected);
                section.addEntry(new Reference("DetectedIssue/" + detected.getIdElement().getIdPart()));
            }
            reports.add(report);
            composition.addSection(section);
        }

        careGapReport.addEntry(new Bundle.BundleEntryComponent().setResource(composition));
        // Add the reports based on status parameter
        boolean addreport;
        for (MeasureReport rep : reports) {
            if (getMeasureScore(rep) != null) {
                boolean openGap = checkOpenGap(rep);
                if (status == null || status == "") {
                    addreport = true;
                } else if (status.equals("open-gap") && openGap) {
                    addreport = true;
                } else if (status.equals("closed-gap") && !openGap) {
                    addreport = true;
                } else {
                    addreport = false;
                }
                if (addreport) {
                    Parameters parameters = addEvaluatedResources(rep);
                    careGapReport.addEntry(new Bundle.BundleEntryComponent().setResource(rep));
                    for (DetectedIssue detectedIssue : detectedIssues) {
                        careGapReport.addEntry(new Bundle.BundleEntryComponent().setResource(detectedIssue));
                    }
                    for (Parameters.ParametersParameterComponent parameter : parameters.getParameter()) {
                        careGapReport.addEntry(new Bundle.BundleEntryComponent().setResource(parameter.getResource()));
                    }

                }
            }

        }

        return careGapReport;

    }

    private Parameters addEvaluatedResources(MeasureReport rep) {
        Parameters parameters = new Parameters();
        if (rep.hasContained()) {
            for (Resource contained : rep.getContained()) {
                if (contained instanceof Bundle) {
                    addEvaluatedResourcesToParameters((Bundle) contained, parameters, null);
                    List<Reference> listRef = populateListReferences((Bundle) contained);
                    rep.setEvaluatedResource(listRef);

                }
            }
        }
        return parameters;
    }

    private List<Reference> populateListReferences(Bundle bundle) {
        List<Reference> listRef = new ArrayList<>();
        if (bundle.hasEntry()) {
            for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
                if ((entry.getResource() instanceof ListResource)) {
                    ListResource listResource = (ListResource) entry.getResource();
                    String popType = listResource.getTitle();
                    String displayPop = WordUtils.capitalize(popType.replace("-", " "));
                    for (ListEntryComponent listComponent : listResource.getEntry()) {
                        List<Extension> evalResourceExt = new ArrayList<>();
                        evalResourceExt.add(new Extension(
                                "http://hl7.org/fhir/us/davinci-deqm/StructureDefinition/extension-populationReference",
                                new CodeableConcept()
                                        .addCoding(new Coding("http://teminology.hl7.org/CodeSystem/measure-population",
                                                popType, displayPop))));
                        Reference evaluateRef = listComponent.getItem();
                        evaluateRef.setExtension(evalResourceExt);
                        listRef.add(evaluateRef);
                    }
                }
            }
        }
        return listRef;
    }

    private DetectedIssue checkDetectedIssue(MeasureReport report, String patientRef) {
        boolean isOpenGap = checkOpenGap(report);

        if (isOpenGap) {
            return createDetectedIssue(report, patientRef);
        } else {
            return null;
        }
    }

    private boolean checkOpenGap(MeasureReport report) {
        boolean openGap = false;
        String improvementNotation = report.getImprovementNotation().getCodingFirstRep().getCode().toLowerCase();
        BigDecimal measureScore = getMeasureScore(report);
        if (measureScore != null) {
            if (((improvementNotation.equals("increase")) && (measureScore.compareTo(new BigDecimal(0))) <= 0)) {
                openGap = true;
            } else if (((improvementNotation.equals("decrease")) && (measureScore.compareTo(new BigDecimal(1))) >= 0)) {
                openGap = true;
            }
        }

        return openGap;
    }

    private DetectedIssue createDetectedIssue(MeasureReport report, String patientRef) {
        DetectedIssue detectedIssue = new DetectedIssue();
        detectedIssue.setId(UUID.randomUUID().toString());
        detectedIssue.setStatus(DetectedIssue.DetectedIssueStatus.FINAL);
        detectedIssue.setPatient(new Reference("Patient/" + patientRef));
        detectedIssue.getEvidence().add(new DetectedIssue.DetectedIssueEvidenceComponent()
                .addDetail(new Reference("MeasureReport/" + report.getId())));
        CodeableConcept code = new CodeableConcept().addCoding(
                new Coding().setSystem("http://hl7.org/fhir/us/davinci-deqm/CodeSystem/detectedissue-category")
                        .setCode("care-gap").setDisplay("Gap in Care Detected"));
        detectedIssue.setCode(code);
        return detectedIssue;
    }

    private BigDecimal getMeasureScore(MeasureReport report) {
        BigDecimal scorevalue = null;
        for (MeasureReport.MeasureReportGroupComponent group : report.getGroup()) {
            if (group.hasMeasureScore()) {
                scorevalue = group.getMeasureScore().getValue();
            }
        }
        return scorevalue;
    }

    private List<String> getGroupPatients(String groupRef, DataProvider provider) {
        if (groupRef.startsWith("Group/")) {
            groupRef = groupRef.replace("Group/", "");
        }
        List<String> patients = new ArrayList<>();
        System.out.println("Search for patients in the group: " + groupRef);
        Iterable<Object> groupRetrieve = provider.retrieve("Group", "id", groupRef, "Group", null, null, null, null,
                null, null, null, null);

        for (Iterator iterator = groupRetrieve.iterator(); iterator.hasNext();) {
            Group group = (Group) iterator.next();
            if (group.getIdElement().getIdPart().equals(groupRef)) {
                List<GroupMemberComponent> memberList = group.getMember();
                for (GroupMemberComponent member : memberList) {
                    patients.add(member.getEntity().getReference());
                }
            }

        }
        // logger.info("patients available!!" + patients.size());

        return patients;
    }

    private List<String> getPractitionerPatients(String practitionerRef, DataProvider provider) {
        SearchParameterMap map = new SearchParameterMap();
        map.add("general-practitioner", new ReferenceParam(
                practitionerRef.startsWith("Practitioner/") ? practitionerRef : "Practitioner/" + practitionerRef));

        List<String> patients = new ArrayList<>();
        IBundleProvider patientProvider = registry.getResourceDao("Patient").search(map);
        List<IBaseResource> patientList = patientProvider.getResources(0, patientProvider.size());
        patientList.forEach(x -> patients.add((String) x.getIdElement().getIdPart()));
        return patients;
    }

    private List<String> getPatients(String patientRef, DataProvider provider) {
        if (patientRef.startsWith("Patient/")) {
            patientRef = patientRef.replace("Patient/", "");
        }
        List<String> patients = new ArrayList<>();
        Iterable<Object> patientRetrieve = provider.retrieve("Patient", "id", patientRef, "Patient", null, null, null,
                null, null, null, null, null);

        for (Iterator iterator = patientRetrieve.iterator(); iterator.hasNext();) {
            Patient patient = (Patient) iterator.next();
            patients.add(patient.getIdElement().getIdPart());
        }
        // logger.info("patients available!!" + patients.size());

        return patients;
    }

    @Operation(name = "$collect-data", idempotent = true, type = Measure.class)
    public Parameters collectData(@IdParam IdType theId, @OperationParam(name = "periodStart") String periodStart,
            @OperationParam(name = "periodEnd") String periodEnd, @OperationParam(name = "subject") String patientRef,
            @OperationParam(name = "practitioner") String practitionerRef,
            @OperationParam(name = "patientServerUrl") String patientServerUrl,
            @OperationParam(name = "patientServerToken") String patientServerToken,
            @OperationParam(name = "dataBundle", min = 1, max = 1, type = Bundle.class) Bundle dataBundle,
            @OperationParam(name = "lastReceivedOn") String lastReceivedOn) throws FHIRException {
        // TODO: Spec says that the periods are not required, but I am not sure what to
        // do when they aren't supplied so I made them required
        MeasureReport report = evaluateMeasure(theId, periodStart, periodEnd, null, null, patientRef, null,
                practitionerRef, lastReceivedOn, patientServerUrl, patientServerToken, dataBundle, null, null, null);
        report.setGroup(null);

        Parameters parameters = new Parameters();

        parameters.addParameter(
                new Parameters.ParametersParameterComponent().setName("measureReport").setResource(report));

        if (report.hasContained()) {
            for (Resource contained : report.getContained()) {
                if (contained instanceof Bundle) {
                    addEvaluatedResourcesToParameters((Bundle) contained, parameters, dataBundle);
                }
            }
        }

        // TODO: need a way to resolve referenced resources within the evaluated
        // resources
        // Should be able to use _include search with * wildcard, but HAPI doesn't
        // support that

        return parameters;
    }

    private void addEvaluatedResourcesToParameters(Bundle contained, Parameters parameters, Bundle dataBundle) {
        Map<String, Resource> resourceMap = new HashMap<>();
        if (contained.hasEntry()) {
            for (Bundle.BundleEntryComponent entry : contained.getEntry()) {
                if (entry.hasResource() && !(entry.getResource() instanceof ListResource)) {
                    if (!resourceMap.containsKey(entry.getResource().getIdElement().getValue())) {
                        parameters.addParameter(new Parameters.ParametersParameterComponent().setName("resource")
                                .setResource(entry.getResource()));

                        resourceMap.put(entry.getResource().getIdElement().getValue(), entry.getResource());
                        // Commenting for now to resolve bug for remotefhir care-gap
                        // resolveReferences(entry.getResource(), parameters, resourceMap, dataBundle);
                    }
                }
            }
        }
    }

    private void resolveReferences(Resource resource, Parameters parameters, Map<String, Resource> resourceMap,
            Bundle dataBundle) {
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
                Resource fetchedResource;
                if (dataBundle != null) {
                    fetchedResource = (Resource) getRelatedResource(
                            ((Reference) values.get(0)).getReferenceElement().getResourceType(),
                            ((Reference) values.get(0)).getReferenceElement().getIdPart(), dataBundle);

                } else {
                    fetchedResource = (Resource) registry
                            .getResourceDao(((Reference) values.get(0)).getReferenceElement().getResourceType())
                            .read(new IdType(((Reference) values.get(0)).getReferenceElement().getIdPart()));
                }

                if (!resourceMap.containsKey(fetchedResource.getIdElement().getValue())) {
                    parameters.addParameter(new Parameters.ParametersParameterComponent().setName("resource")
                            .setResource(fetchedResource));

                    resourceMap.put(fetchedResource.getIdElement().getValue(), fetchedResource);
                }
            }
        }
    }

    private Resource getRelatedResource(String resourceType, String idPart, Bundle dataBundle) {
        logger.info("Resource type :" + resourceType);
        logger.info("idPart :" + idPart);
        for (Bundle.BundleEntryComponent entry : dataBundle.getEntry()) {
            if (entry.getResource().getResourceType().toString().equals(resourceType)) {
                if (entry.getResource().getIdElement().getIdPart().equals(idPart)) {
                    return entry.getResource();
                }
            }
        }
        throw new RuntimeException("Resource " + idPart + " not found");
    }

    // TODO - this needs a lot of work
    @Operation(name = "$data-requirements", idempotent = true, type = Measure.class)
    public org.hl7.fhir.r4.model.Library dataRequirements(@IdParam IdType theId,
            @OperationParam(name = "startPeriod") String startPeriod,
            @OperationParam(name = "endPeriod") String endPeriod) throws InternalErrorException, FHIRException {

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
        System.out.println(FhirContext.forR4().newJsonParser().encodeResourceToString(transactionBundle));
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
