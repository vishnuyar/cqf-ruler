package org.opencds.cqf.providers;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.cqframework.cql.elm.execution.CodeDef;
import org.cqframework.cql.elm.execution.CodeSystemDef;
import org.cqframework.cql.elm.execution.ExpressionDef;
import org.cqframework.cql.elm.execution.FunctionDef;
import org.cqframework.cql.elm.execution.IncludeDef;
import org.cqframework.cql.elm.execution.Library;
import org.cqframework.cql.elm.execution.ValueSetDef;
import org.cqframework.cql.elm.execution.VersionedIdentifier;
import org.hl7.elm.r1.Library.Statements;
import org.hl7.fhir.dstu3.model.Annotation;
import org.hl7.fhir.dstu3.model.Attachment;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.CodeableConcept;
import org.hl7.fhir.dstu3.model.Coding;
import org.hl7.fhir.dstu3.model.Composition;
import org.hl7.fhir.dstu3.model.DataRequirement;
import org.hl7.fhir.dstu3.model.DataRequirement.DataRequirementCodeFilterComponent;
import org.hl7.fhir.dstu3.model.IdType;
import org.hl7.fhir.dstu3.model.ListResource;
import org.hl7.fhir.dstu3.model.Measure;
import org.hl7.fhir.dstu3.model.MeasureReport;
import org.hl7.fhir.dstu3.model.Narrative;
import org.hl7.fhir.dstu3.model.ParameterDefinition;
import org.hl7.fhir.dstu3.model.Parameters;
import org.hl7.fhir.dstu3.model.Reference;
import org.hl7.fhir.dstu3.model.RelatedArtifact;
import org.hl7.fhir.dstu3.model.Resource;
import org.hl7.fhir.dstu3.model.StringType;
import org.hl7.fhir.dstu3.model.Measure.MeasureGroupComponent;
import org.hl7.fhir.dstu3.model.Measure.MeasureGroupPopulationComponent;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.instance.model.api.IAnyResource;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.utilities.xhtml.XhtmlNode;
import org.opencds.cqf.config.STU3LibraryLoader;
import org.opencds.cqf.evaluation.MeasureEvaluation;
import org.opencds.cqf.evaluation.MeasureEvaluationSeed;
import org.opencds.cqf.helpers.LibraryHelper;
import org.opencds.cqf.helpers.LibraryResourceHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.jpa.dao.IFhirSystemDao;
import ca.uhn.fhir.jpa.rp.dstu3.LibraryResourceProvider;
import ca.uhn.fhir.jpa.rp.dstu3.MeasureResourceProvider;
import ca.uhn.fhir.jpa.searchparam.SearchParameterMap;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.annotation.OptionalParam;
import ca.uhn.fhir.rest.annotation.RequiredParam;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.api.RestOperationTypeEnum;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.param.TokenParam;
import ca.uhn.fhir.rest.param.TokenParamModifier;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;

public class FHIRMeasureResourceProvider extends MeasureResourceProvider {

    private JpaDataProvider provider;
    private IFhirSystemDao systemDao;

    private NarrativeProvider narrativeProvider;
    private HQMFProvider hqmfProvider;

    private LibraryResourceProvider libraryResourceProvider;

    private static final Logger logger = LoggerFactory.getLogger(FHIRMeasureResourceProvider.class);

    public FHIRMeasureResourceProvider(JpaDataProvider dataProvider, IFhirSystemDao systemDao, NarrativeProvider narrativeProvider, HQMFProvider hqmfProvider) {
        this.provider = dataProvider;
        this.systemDao = systemDao;

        this.libraryResourceProvider = (LibraryResourceProvider) dataProvider.resolveResourceProvider("Library");
        this.narrativeProvider = narrativeProvider;
        this.hqmfProvider = hqmfProvider;
    }

    @Operation(name="$hqmf", idempotent = true)
    public Parameters hqmf(@IdParam IdType theId) {
        Measure theResource = this.getDao().read(theId);

        STU3LibraryLoader libraryLoader = LibraryHelper.createLibraryLoader(this.libraryResourceProvider);
        String hqmf = this.generateHQMF(theResource, libraryLoader);
        Parameters p = new Parameters();
        p.addParameter().setValue(new StringType(hqmf));
        return p;
    }


    @Operation(name="$refresh-generated-content")
    public MethodOutcome refreshGeneratedContent(HttpServletRequest theRequest, RequestDetails theRequestDetails, @IdParam IdType theId) {
        Measure theResource = this.getDao().read(theId);
        STU3LibraryLoader libraryLoader = LibraryHelper.createLibraryLoader(this.libraryResourceProvider);

        CqfMeasure cqfMeasure = this.createCqfMeasure(theResource, libraryLoader);

        //Ensure All Related Artifacts for all referenced Libraries
        if (!cqfMeasure.getRelatedArtifact().isEmpty()) {
            for (RelatedArtifact relatedArtifact : cqfMeasure.getRelatedArtifact()) {
                boolean artifactExists = false;
                //logger.info("Related Artifact: " + relatedArtifact.getUrl());
                for (RelatedArtifact resourceArtifact : theResource.getRelatedArtifact()) {
                    if (resourceArtifact.equalsDeep(relatedArtifact)) {
                        //logger.info("Equals deep true");
                        artifactExists = true;
                        break;
                    }
                }
                if (!artifactExists) {
                    theResource.addRelatedArtifact(relatedArtifact.copy());
                }
            }
        }

        Narrative n = this.narrativeProvider.getNarrative(this.getContext(), cqfMeasure);
        theResource.setText(n.copy());
        //logger.info("Narrative: " + n.getDivAsString());
        return super.update(theRequest, theResource, theId, theRequestDetails.getConditionalUrl(RestOperationTypeEnum.UPDATE), theRequestDetails);
    }

    @Operation(name="$get-narrative", idempotent = true)
    public Parameters getNarrative(@IdParam IdType theId) {
        Measure theResource = this.getDao().read(theId);
        STU3LibraryLoader libraryLoader = LibraryHelper.createLibraryLoader(this.libraryResourceProvider);
        CqfMeasure cqfMeasure = this.createCqfMeasure(theResource, libraryLoader);
        Narrative n = this.narrativeProvider.getNarrative(this.getContext(), cqfMeasure);
        Parameters p = new Parameters();
        p.addParameter().setValue(new StringType(n.getDivAsString()));
        return p;
    }

    private String generateHQMF(Measure theResource, STU3LibraryLoader libraryLoader) {
        CqfMeasure cqfMeasure = this.createCqfMeasure(theResource, libraryLoader);
        return this.hqmfProvider.generateHQMF(cqfMeasure);
    }

    /*
    *
    * NOTE that the source, user, and pass parameters are not standard parameters for the FHIR $evaluate-measure operation
    *
    * */
    @Operation(name = "$evaluate-measure", idempotent = true)
    public MeasureReport evaluateMeasure(
            @IdParam IdType theId,
            @RequiredParam(name="periodStart") String periodStart,
            @RequiredParam(name="periodEnd") String periodEnd,
            @OptionalParam(name="measure") String measureRef,
            @OptionalParam(name="reportType") String reportType,
            @OptionalParam(name="patient") String patientRef,
            @OptionalParam(name="practitioner") String practitionerRef,
            @OptionalParam(name="lastReceivedOn") String lastReceivedOn,
            @OptionalParam(name="source") String source,
            @OptionalParam(name="user") String user,
            @OptionalParam(name="pass") String pass) throws InternalErrorException, FHIRException
    {
        STU3LibraryLoader libraryLoader = LibraryHelper.createLibraryLoader(this.libraryResourceProvider);
        MeasureEvaluationSeed seed = new MeasureEvaluationSeed(provider, libraryLoader);
        Measure measure = this.getDao().read(theId);

        if (measure == null)
        {
            throw new RuntimeException("Could not find Measure/" + theId.getIdPart());
        }

        seed.setup(measure, periodStart, periodEnd, source, user, pass);


        // resolve report type
        MeasureEvaluation evaluator = new MeasureEvaluation(seed.getDataProvider(), seed.getMeasurementPeriod());
        boolean isQdm = seed.getDataProvider() instanceof Qdm54DataProvider;
        if (reportType != null) {
            switch (reportType) {
                case "patient": return isQdm ? evaluator.evaluateQdmPatientMeasure(seed.getMeasure(), seed.getContext(), patientRef) : evaluator.evaluatePatientMeasure(seed.getMeasure(), seed.getContext(), patientRef);
                case "patient-list": return  evaluator.evaluatePatientListMeasure(seed.getMeasure(), seed.getContext(), practitionerRef);
                case "population": return isQdm ? evaluator.evaluateQdmPopulationMeasure(seed.getMeasure(), seed.getContext()) : evaluator.evaluatePopulationMeasure(seed.getMeasure(), seed.getContext());
                default: throw new IllegalArgumentException("Invalid report type: " + reportType);
            }
        }

        // default report type is patient
        return isQdm ? evaluator.evaluateQdmPatientMeasure(seed.getMeasure(), seed.getContext(), patientRef) : evaluator.evaluatePatientMeasure(seed.getMeasure(), seed.getContext(), patientRef);
    }

    @Operation(name = "$evaluate-measure-with-source", idempotent = true)
    public MeasureReport evaluateMeasure(
            @IdParam IdType theId,
            @OperationParam(name="sourceData", min = 1, max = 1, type = Bundle.class) Bundle sourceData,
            @OperationParam(name="periodStart", min = 1, max = 1) String periodStart,
            @OperationParam(name="periodEnd", min = 1, max = 1) String periodEnd)
    {
        if (periodStart == null || periodEnd == null) {
            throw new IllegalArgumentException("periodStart and periodEnd are required for measure evaluation");
        }
        STU3LibraryLoader libraryLoader = LibraryHelper.createLibraryLoader(this.libraryResourceProvider);
        MeasureEvaluationSeed seed = new MeasureEvaluationSeed(provider, libraryLoader);
        Measure measure = this.getDao().read(theId);

        if (measure == null)
        {
            throw new RuntimeException("Could not find Measure/" + theId.getIdPart());
        }

        seed.setup(measure, periodStart, periodEnd, null, null, null);
        BundleDataProviderStu3 bundleProvider = new BundleDataProviderStu3(sourceData);
        bundleProvider.setTerminologyProvider(provider.getTerminologyProvider());
        seed.getContext().registerDataProvider("http://hl7.org/fhir", bundleProvider);
        MeasureEvaluation evaluator = new MeasureEvaluation(bundleProvider, seed.getMeasurementPeriod());
        return evaluator.evaluatePatientMeasure(seed.getMeasure(), seed.getContext(), "");
    }

    @Operation(name = "$care-gaps", idempotent = true)
    public Bundle careGapsReport(
            @RequiredParam(name="periodStart") String periodStart,
            @RequiredParam(name="periodEnd") String periodEnd,
            @RequiredParam(name="topic") String topic,
            @RequiredParam(name="patient") String patientRef
    ) {
        List<IBaseResource> measures = getDao().search(new SearchParameterMap().add("topic", new TokenParam().setModifier(TokenParamModifier.TEXT).setValue(topic))).getResources(0, 1000);
        Bundle careGapReport = new Bundle();
        careGapReport.setType(Bundle.BundleType.DOCUMENT);

        Composition composition = new Composition();
        // TODO - this is a placeholder code for now ... replace with preferred code once identified
        CodeableConcept typeCode = new CodeableConcept().addCoding(new Coding().setSystem("http://loinc.org").setCode("57024-2"));
        composition.setStatus(Composition.CompositionStatus.FINAL)
                .setType(typeCode)
                .setSubject(new Reference(patientRef.startsWith("Patient/") ? patientRef : "Patient/" + patientRef))
                .setTitle(topic + " Care Gap Report");

        List<MeasureReport> reports = new ArrayList<>();
        MeasureReport report = new MeasureReport();
        for (IBaseResource resource : measures) {
            Composition.SectionComponent section = new Composition.SectionComponent();

            Measure measure = (Measure) resource;
            section.addEntry(new Reference(measure.getIdElement().getResourceType() + "/" + measure.getIdElement().getIdPart()));
            if (measure.hasTitle()) {
                section.setTitle(measure.getTitle());
            }
            String improvementNotation = "increase"; // defaulting to "increase"
            if (measure.hasImprovementNotation()) {
                improvementNotation = measure.getImprovementNotation();
                section.setText(
                        new Narrative()
                                .setStatus(Narrative.NarrativeStatus.GENERATED)
                                .setDiv(new XhtmlNode().setValue(improvementNotation))
                );
            }

            STU3LibraryLoader libraryLoader = LibraryHelper.createLibraryLoader(this.libraryResourceProvider);
            MeasureEvaluationSeed seed = new MeasureEvaluationSeed(provider, libraryLoader);
            seed.setup(measure, periodStart, periodEnd, null, null, null);
            MeasureEvaluation evaluator = new MeasureEvaluation(seed.getDataProvider(), seed.getMeasurementPeriod());
            // TODO - this is configured for patient-level evaluation only
            report = evaluator.evaluatePatientMeasure(seed.getMeasure(), seed.getContext(), patientRef);

            if (report.hasGroup() && measure.hasScoring()) {
                int numerator = 0;
                int denominator = 0;
                for (MeasureReport.MeasureReportGroupComponent group : report.getGroup()) {
                    if (group.hasPopulation()) {
                        for (MeasureReport.MeasureReportGroupPopulationComponent population : group.getPopulation()) {
                            // TODO - currently configured for measures with only 1 numerator and 1 denominator
                            if (population.hasCode()) {
                                if (population.getCode().hasCoding()) {
                                    for (Coding coding : population.getCode().getCoding()) {
                                        if (coding.hasCode()) {
                                            if (coding.getCode().equals("numerator") && population.hasCount()) {
                                                numerator = population.getCount();
                                            }
                                            else if (coding.getCode().equals("denominator") && population.hasCount()) {
                                                denominator = population.getCount();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                double proportion = 0.0;
                if (measure.getScoring().hasCoding() && denominator != 0) {
                    for (Coding coding : measure.getScoring().getCoding()) {
                        if (coding.hasCode() && coding.getCode().equals("proportion")) {
                            proportion = numerator / denominator;
                        }
                    }
                }

                // TODO - this is super hacky ... change once improvementNotation is specified as a code
                if (improvementNotation.toLowerCase().contains("increase")) {
                    if (proportion < 1.0) {
                        composition.addSection(section);
                        reports.add(report);
                    }
                }
                else if (improvementNotation.toLowerCase().contains("decrease")) {
                    if (proportion > 0.0) {
                        composition.addSection(section);
                        reports.add(report);
                    }
                }

                // TODO - add other types of improvement notation cases
            }
        }

        careGapReport.addEntry(new Bundle.BundleEntryComponent().setResource(composition));

        for (MeasureReport rep : reports) {
            careGapReport.addEntry(new Bundle.BundleEntryComponent().setResource(rep));
        }

        return careGapReport;
    }

    @Operation(name = "$collect-data", idempotent = true)
    public Parameters collectData(
            @IdParam IdType theId,
            @RequiredParam(name="periodStart") String periodStart,
            @RequiredParam(name="periodEnd") String periodEnd,
            @OptionalParam(name="patient") String patientRef,
            @OptionalParam(name="practitioner") String practitionerRef,
            @OptionalParam(name="lastReceivedOn") String lastReceivedOn
    ) throws FHIRException
    {
        // TODO: Spec says that the periods are not required, but I am not sure what to do when they aren't supplied so I made them required
        MeasureReport report = evaluateMeasure(theId, periodStart, periodEnd, null, null, patientRef, practitionerRef, lastReceivedOn, null, null, null);
        report.setGroup(null);

        Parameters parameters = new Parameters();

        parameters.addParameter(
                new Parameters.ParametersParameterComponent().setName("measurereport").setResource(report)
        );

        if (report.hasContained())
        {
            for (Resource contained : report.getContained())
            {
                if (contained instanceof Bundle)
                {
                    addEvaluatedResourcesToParameters((Bundle) contained, parameters);
                }
            }
        }

        // TODO: need a way to resolve referenced resources within the evaluated resources
        // Should be able to use _include search with * wildcard, but HAPI doesn't support that

        return parameters;
    }

    private void addEvaluatedResourcesToParameters(Bundle contained, Parameters parameters)
    {
        Map<String, Resource> resourceMap = new HashMap<>();
        if (contained.hasEntry())
        {
            for (Bundle.BundleEntryComponent entry : contained.getEntry())
            {
                if  (entry.hasResource() && !(entry.getResource() instanceof ListResource))
                {
                    if (!resourceMap.containsKey(entry.getResource().getIdElement().getValue()))
                    {
                        parameters.addParameter(
                                new Parameters.ParametersParameterComponent().setName("resource").setResource(entry.getResource())
                        );

                        resourceMap.put(entry.getResource().getIdElement().getValue(), entry.getResource());

                        resolveReferences(entry.getResource(), parameters, resourceMap);
                    }
                }
            }
        }
    }

    private void resolveReferences(Resource resource, Parameters parameters, Map<String, Resource> resourceMap)
    {
        List<IBase> values;
        for (BaseRuntimeChildDefinition child : getContext().getResourceDefinition(resource).getChildren())
        {
            values = child.getAccessor().getValues(resource);
            if (values == null || values.isEmpty())
            {
                continue;
            }

            else if (values.get(0) instanceof Reference
                    && ((Reference) values.get(0)).getReferenceElement().hasResourceType()
                    && ((Reference) values.get(0)).getReferenceElement().hasIdPart())
            {
                Resource fetchedResource =
                        (Resource) provider.resolveResourceProvider(
                                ((Reference) values.get(0)).getReferenceElement().getResourceType()
                        ).getDao().read(
                                new IdType(((Reference) values.get(0)).getReferenceElement().getIdPart())
                        );

                if (!resourceMap.containsKey(fetchedResource.getIdElement().getValue()))
                {
                    parameters.addParameter(
                            new Parameters.ParametersParameterComponent()
                                    .setName("resource")
                                    .setResource(fetchedResource)
                    );

                    resourceMap.put(fetchedResource.getIdElement().getValue(), fetchedResource);
                }
            }
        }
    }

    // TODO - this needs a lot of work
    @Operation(name = "$data-requirements", idempotent = true)
    public org.hl7.fhir.dstu3.model.Library dataRequirements(
            @IdParam IdType theId,
            @RequiredParam(name="startPeriod") String startPeriod,
            @RequiredParam(name="endPeriod") String endPeriod)
            throws InternalErrorException, FHIRException
    {
        STU3LibraryLoader libraryLoader = LibraryHelper.createLibraryLoader(this.libraryResourceProvider);
        Measure measure = this.getDao().read(theId);

        return getDataRequirements(measure, libraryLoader);
    }

    protected org.hl7.fhir.dstu3.model.Library getDataRequirements(Measure measure, STU3LibraryLoader libraryLoader){
        LibraryHelper.loadLibraries(measure, libraryLoader, this.libraryResourceProvider);

        List<DataRequirement> reqs = new ArrayList<>();
        List<RelatedArtifact> dependencies = new ArrayList<>();
        List<ParameterDefinition> parameters = new ArrayList<>();

        for (Library library : libraryLoader.getLibraries()) {
            VersionedIdentifier primaryLibraryIdentifier = library.getIdentifier();
            org.hl7.fhir.dstu3.model.Library libraryResource = LibraryResourceHelper.resolveLibraryByName(
                (LibraryResourceProvider)provider.resolveResourceProvider("Library"),
                primaryLibraryIdentifier.getId(),
                primaryLibraryIdentifier.getVersion());
    
            for (RelatedArtifact dependency : libraryResource.getRelatedArtifact()) {
                if (dependency.getType().toCode().equals("depends-on")) {
                    dependencies.add(dependency);
                }
            }

            reqs.addAll(libraryResource.getDataRequirement());
            parameters.addAll(libraryResource.getParameter());
        }

        List<Coding> typeCoding = new ArrayList<>();
        typeCoding.add(new Coding().setCode("module-definition"));
        org.hl7.fhir.dstu3.model.Library library =
                new org.hl7.fhir.dstu3.model.Library().setType(new CodeableConcept().setCoding(typeCoding));

        if (!dependencies.isEmpty()) {
            library.setRelatedArtifact(dependencies);
        }

        if (!reqs.isEmpty()) {
            library.setDataRequirement(reqs);
        }

        if (!parameters.isEmpty()) {
            library.setParameter(parameters);
        }

        return library;
    }

    @Operation(name = "$submit-data", idempotent = true)
    public Resource submitData(
            RequestDetails details,
            @IdParam IdType theId,
            @OperationParam(name="measure-report", min = 1, max = 1, type = MeasureReport.class) MeasureReport report,
            @OperationParam(name="resource") List<IAnyResource> resources)
    {
        Bundle transactionBundle = new Bundle().setType(Bundle.BundleType.TRANSACTION);

        /*
            TODO - resource validation using $data-requirements operation
            (params are the provided id and the measurement period from the MeasureReport)

            TODO - profile validation ... not sure how that would work ...
            (get StructureDefinition from URL or must it be stored in Ruler?)
        */

        transactionBundle.addEntry(createTransactionEntry(report));

        for (IAnyResource resource : resources) {
            Resource res = (Resource) resource;
            if (res instanceof Bundle) {
                for (Bundle.BundleEntryComponent entry : createTransactionBundle((Bundle) res).getEntry()) {
                    transactionBundle.addEntry(entry);
                }
            }
            else {
                // Build transaction bundle
                transactionBundle.addEntry(createTransactionEntry(res));
            }
        }

        return (Resource) systemDao.transaction(details, transactionBundle);
    }

    private CqfMeasure createCqfMeasure(Measure measure, STU3LibraryLoader libraryLoader) {
        CqfMeasure cqfMeasure = new CqfMeasure(measure);

        //Ensure All Data Requirements for all referenced libraries
        org.hl7.fhir.dstu3.model.Library moduleDefinition = this.getDataRequirements(measure, libraryLoader);

        cqfMeasure.setRelatedArtifact(moduleDefinition.getRelatedArtifact());
        cqfMeasure.setDataRequirement(moduleDefinition.getDataRequirement());
        cqfMeasure.setParameter(moduleDefinition.getParameter());

        ArrayList<MeasureGroupComponent> populationStatements = new ArrayList<>();
        for (MeasureGroupComponent group : measure.getGroup()) {
            populationStatements.add(group.copy());
        }
        ArrayList<MeasureGroupPopulationComponent> definitionStatements = new ArrayList<>();
        ArrayList<MeasureGroupPopulationComponent> functionStatements = new ArrayList<>();
        ArrayList<StringType> terminology = new ArrayList<>();
        ArrayList<StringType> codes = new ArrayList<>();
        ArrayList<StringType> codeSystems = new ArrayList<>();
        ArrayList<StringType> valueSets = new ArrayList<>();
        ArrayList<StringType> dataCriteria = new ArrayList<>();

        Library primaryLibrary = LibraryHelper.resolvePrimaryLibrary(measure, libraryLoader);
        String primaryLibraryCql = "";

        for (Library library : libraryLoader.getLibraries()) {
            Boolean isPrimaryLibrary = library.getIdentifier().getId().equalsIgnoreCase(primaryLibrary.getIdentifier().getId());
            String libraryNamespace = "";
            for (IncludeDef include : primaryLibrary.getIncludes().getDef()) {
                if (library.getIdentifier().getId().equalsIgnoreCase(include.getPath())) {
                    libraryNamespace = include.getLocalIdentifier() + ".";
                }
            }
            VersionedIdentifier libraryIdentifier = library.getIdentifier();
            org.hl7.fhir.dstu3.model.Library libraryResource = LibraryResourceHelper.resolveLibraryByName(
                libraryResourceProvider,
                libraryIdentifier.getId(),
                libraryIdentifier.getVersion());

            String cql = "";
            for (Attachment attachment : libraryResource.getContent()) {
                cqfMeasure.addContent(attachment);
                if (attachment.getContentType().equalsIgnoreCase("text/cql")) {
                    cql = new String(attachment.getData());
                }
            }
            if (isPrimaryLibrary) {
                primaryLibraryCql = cql;
            }
            String[] cqlLines = cql.replaceAll("[\r]", "").split("[\n]");
    
            for (ExpressionDef statement : library.getStatements().getDef()) {
                String[] location = statement.getLocator().split("-");
                String statementText = "";
                String signature = "";
                int start = Integer.parseInt(location[0].split(":")[0]);
                int end = Integer.parseInt(location[1].split(":")[0]);
                for (int i = start - 1; i < end; i++) {
                    if (cqlLines[i].contains("define function \"" + statement.getName() + "\"(")) {
                        signature = cqlLines[i].substring(cqlLines[i].indexOf("("), cqlLines[i].indexOf(")") + 1);
                    }
                    if (!cqlLines[i].contains("define \"" + statement.getName() + "\":") && !cqlLines[i].contains("define function \"" + statement.getName() + "\"(")) {
                        statementText = statementText.concat((statementText.length() > 0 ? "\r\n" : "") + cqlLines[i]);
                    }
                }
                if (statementText.startsWith("context")) {
                    continue;
                }
                MeasureGroupPopulationComponent def = new MeasureGroupPopulationComponent();
                def.setName(libraryNamespace + statement.getName() + signature);
                def.setCriteria(statementText);
                //TODO: Only statements that are directly referenced in the primary library cql will be included.
                if (statement.getClass() == FunctionDef.class) {
                    if (isPrimaryLibrary || primaryLibraryCql.contains(libraryNamespace + "\"" + statement.getName() + "\"")) {
                        functionStatements.add(def);
                    }
                }
                else {
                    if (isPrimaryLibrary || primaryLibraryCql.contains(libraryNamespace + "\"" + statement.getName() + "\"")) {
                        definitionStatements.add(def);
                    }
                }

                for (MeasureGroupComponent group : populationStatements) {
                    for (MeasureGroupPopulationComponent population : group.getPopulation()) {
                        if (population.getCriteria().equalsIgnoreCase(statement.getName())) {
                            population.setName(statement.getName());
                            population.setCriteria(statementText);
                        }
                    }
                }
            }

            for (CodeSystemDef codeSystem : library.getCodeSystems().getDef()) {
                StringType term = new StringType();
                String id = codeSystem.getId().replace("urn:oid:", "");
                String name = codeSystem.getName().split(":")[0];
                String version = codeSystem.getName().split(":")[1];
                term.setValueAsString("codesystem \"" + name + "\" using \"" + id + " version " + version);
                Boolean exists = false;
                for (StringType string : codeSystems) {
                    if (string.getValueAsString().equalsIgnoreCase(term.getValueAsString())) {
                        exists = true;
                    }
                }
                if (!exists) {
                    codeSystems.add(term);
                }
            }

            for (CodeDef code : library.getCodes().getDef()) {
                StringType term = new StringType();
                String id = code.getId();
                String name = code.getName();
                String[] codeSystem = code.getCodeSystem().getName().split(":");
                term.setValueAsString("code \"" + name + "\" using \"" + codeSystem[0] + " version " + codeSystem[1] + " Code (" + id + ")");
                Boolean exists = false;
                for (StringType string : codes) {
                    if (string.getValueAsString().equalsIgnoreCase(term.getValueAsString())) {
                        exists = true;
                    }
                }
                if (!exists) {
                    codes.add(term);
                }
            }
        cqfMeasure.getRelatedArtifact().addAll(libraryResource.getRelatedArtifact());

            for (ValueSetDef valueSet : library.getValueSets().getDef()) {
                StringType term = new StringType();
                String id = valueSet.getId().replace("urn:oid:", "");
                String name = valueSet.getName();
                term.setValueAsString("valueset \"" + name + "\" using \"" + id + "\"");
                Boolean exists = false;
                for (StringType string : valueSets) {
                    if (string.getValueAsString().equalsIgnoreCase(term.getValueAsString())) {
                        exists = true;
                    }
                }
                if (!exists) {
                    valueSets.add(term);
                }

                for (DataRequirement data : cqfMeasure.getDataRequirement()) {
                    String type = data.getType();
                    // .split("[A-Z]");
                    // if (type.contains("Negative")) {
                    //     type = 
                    // }
                    for (DataRequirementCodeFilterComponent filter : data.getCodeFilter()) {
                        if (filter.hasValueSetStringType() && filter.getValueSetStringType().getValueAsString().equalsIgnoreCase(valueSet.getId())) {
                            StringType dataElement = new StringType();
                            dataElement.setValueAsString("\"" + type + ": " + name + "\" using \"" + name + " (" + id + ")");
                            exists = false;
                            for (StringType string : dataCriteria) {
                                if (string.getValueAsString().equalsIgnoreCase(dataElement.getValueAsString())) {
                                    exists = true;
                                }
                            }
                            if (!exists) {
                                dataCriteria.add(dataElement);
                            }
                        }
                    }
                }
            }
        }

        Comparator stringTypeComparator = new Comparator<StringType>() {
            @Override
            public int compare(StringType item, StringType t1) {
                String s1 = item.asStringValue();
                String s2 = t1.asStringValue();
                return s1.compareToIgnoreCase(s2);
            }
        };
        Comparator populationComparator = new Comparator<MeasureGroupPopulationComponent>() {
            @Override
            public int compare(MeasureGroupPopulationComponent item, MeasureGroupPopulationComponent t1) {
                String s1 = item.getName();
                String s2 = t1.getName();
                return s1.compareToIgnoreCase(s2);
            }
        };

        Collections.sort(definitionStatements, populationComparator);
        Collections.sort(functionStatements, populationComparator);
        Collections.sort(codeSystems, stringTypeComparator);
        Collections.sort(codes, stringTypeComparator);
        Collections.sort(valueSets, stringTypeComparator);
        Collections.sort(dataCriteria, stringTypeComparator);

        terminology.addAll(codeSystems);
        terminology.addAll(codes);
        terminology.addAll(valueSets);
        
        cqfMeasure.setPopulationStatements(populationStatements);
        cqfMeasure.setDefinitionStatements(definitionStatements);
        cqfMeasure.setFunctionStatements(functionStatements);
        cqfMeasure.setTerminology(terminology);
        cqfMeasure.setDataCriteria(dataCriteria);

        return cqfMeasure;
    }

    private Bundle createTransactionBundle(Bundle bundle) {
        Bundle transactionBundle;
        if (bundle != null) {
            if (bundle.hasType() && bundle.getType() == Bundle.BundleType.TRANSACTION) {
                transactionBundle = bundle;
            }
            else {
                transactionBundle = new Bundle().setType(Bundle.BundleType.TRANSACTION);
                if (bundle.hasEntry()) {
                    for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
                        if (entry.hasResource()) {
                            transactionBundle.addEntry(createTransactionEntry(entry.getResource()));
                        }
                    }
                }
            }
        }
        else {
            transactionBundle = new Bundle().setType(Bundle.BundleType.TRANSACTION).setEntry(new ArrayList<>());
        }

        return transactionBundle;
    }

    private Bundle.BundleEntryComponent createTransactionEntry(Resource resource) {
        Bundle.BundleEntryComponent transactionEntry = new Bundle.BundleEntryComponent().setResource(resource);
        if (resource.hasId()) {
            transactionEntry.setRequest(
                    new Bundle.BundleEntryRequestComponent()
                            .setMethod(Bundle.HTTPVerb.PUT)
                            .setUrl(resource.getId())
            );
        }
        else {
            transactionEntry.setRequest(
                    new Bundle.BundleEntryRequestComponent()
                            .setMethod(Bundle.HTTPVerb.POST)
                            .setUrl(resource.fhirType())
            );
        }
        return transactionEntry;
    }
}
