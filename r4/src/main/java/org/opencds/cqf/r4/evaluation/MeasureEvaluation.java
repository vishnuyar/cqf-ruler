package org.opencds.cqf.r4.evaluation;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.jpa.api.dao.DaoRegistry;
import ca.uhn.fhir.jpa.searchparam.SearchParameterMap;
import ca.uhn.fhir.model.api.TemporalPrecisionEnum;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.ReferenceParam;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Patient.PatientLinkComponent;
import org.cqframework.cql.elm.execution.ExpressionDef;
import org.cqframework.cql.elm.execution.Library;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.opencds.cqf.common.evaluation.MeasurePopulationType;
import org.opencds.cqf.common.evaluation.MeasureScoring;
import org.opencds.cqf.cql.engine.data.DataProvider;
import org.opencds.cqf.cql.engine.execution.Context;
import org.opencds.cqf.cql.engine.runtime.Interval;
import org.opencds.cqf.cql.engine.runtime.Date;
import org.opencds.cqf.r4.builders.MeasureReportBuilder;
import org.opencds.cqf.r4.helpers.FhirMeasureBundler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;

public class MeasureEvaluation {

    private static final Logger logger = LoggerFactory.getLogger(MeasureEvaluation.class);

    private DataProvider provider;
    private Interval measurementPeriod;
    private DaoRegistry registry;

    public MeasureEvaluation(DataProvider provider, DaoRegistry registry, Interval measurementPeriod) {
        this.provider = provider;
        this.registry = registry;
        this.measurementPeriod = measurementPeriod;
    }

    public MeasureReport evaluatePatientMeasure(Measure measure, Context context, String patientId) {
        logger.info("Generating individual report");

        if (patientId == null) {
            return evaluatePopulationMeasure(measure, context);
        }

        Iterable<Object> patientRetrieve = provider.retrieve("Patient", "id", patientId, "Patient", null, null, null,
                null, null, null, null, null);
        Patient patient = null;
        if (patientRetrieve.iterator().hasNext()) {
            patient = (Patient) patientRetrieve.iterator().next();
        }

        return evaluate(measure, context,
                patient == null ? Collections.emptyList() : Collections.singletonList(patient),
                MeasureReport.MeasureReportType.INDIVIDUAL);
    }

    public MeasureReport evaluateSubjectListMeasure(Measure measure, Context context, String practitionerRef) {
        logger.info("Generating patient-list report");

        List<Patient> patients = practitionerRef == null ? getAllPatients() : getPractitionerPatients(practitionerRef);
        logger.info("number of patients: " + patients.size());

        return evaluate(measure, context, patients, MeasureReport.MeasureReportType.SUBJECTLIST);
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

    private List<Patient> getAllPatients() {
        List<Patient> patients = new ArrayList<>();
        Iterable<Object> patientRetrieve = provider.retrieve("Patient", "id", null, "Patient", null, null, null, null,
                null, null, null, null);

        for (Iterator iterator = patientRetrieve.iterator(); iterator.hasNext();) {
            Patient patient = (Patient) iterator.next();
            patients.add(patient);
        }
        // logger.info("patients available!!" + patients.size());

        return patients;
    }

    public MeasureReport evaluatePopulationMeasure(Measure measure, Context context) {
        logger.info("Generating summary report");

        return evaluate(measure, context, getAllPatients(), MeasureReport.MeasureReportType.SUMMARY);
    }

    public Parameters cqlEvaluate(Context context, String patientId, ArrayList<String> criteriaList, Library lib) {
        Parameters parameters = new Parameters();
        ArrayList<String> cqldef = new ArrayList<String>();
        for (String criteria : criteriaList) {
            if (criteria.equals("EvaluateCQL")) {
                for (ExpressionDef expressionDef : lib.getStatements().getDef()) {
                    // System.out.println("Expression Type :" + expressionDef.getClass().getName());
                    // System.out.println("Expression :" + expressionDef.getName());
                    if (!(expressionDef instanceof org.cqframework.cql.elm.execution.FunctionDef)) {
                        cqldef.add(expressionDef.getName());
                    }

                }
            } else {
                cqldef.add(criteria);

            }
        }

        for (String cqlcriteria : cqldef) {
            Object cqlResult = evaluateCqlExpression(context, patientId, cqlcriteria);

            if (cqlResult == null) {
                logger.info("Got null result");
                parameters.addParameter(cqlcriteria, "");
            } else {
                logger.info("Got result of type: " + cqlResult.getClass());
                if (cqlResult instanceof ArrayList) {
                    Boolean isResource = false;
                    Bundle bundle = new Bundle();
                    ArrayList<String> arrayValues = new ArrayList<>();
                    int count = 0;
                    for (Object obj : (Iterable<Object>) cqlResult) {
                        count += 1;
                        // System.out.println("The count of obj is: " + count);
                        if (obj instanceof Resource) {
                            isResource = true;
                            bundle.addEntry(new Bundle.BundleEntryComponent().setResource((Resource) obj));
                        } else {
                            if (obj != null) {

                                if (obj instanceof Patient.PatientLinkComponent) {
                                    PatientLinkComponent linkComp = (PatientLinkComponent) obj;
                                    System.out.println("link comp reference : " + linkComp.getOther().getReference());
                                    Reference linkRef = new Reference();
                                    linkRef.setReference(linkComp.getOther().getReference());
                                    Parameters.ParametersParameterComponent pc = new Parameters.ParametersParameterComponent()
                                            .setName(cqlcriteria);
                                    pc.setValue(linkRef);
                                    parameters.addParameter(pc);

                                } else {
                                    System.out.println("Object of type" + obj.getClass());
                                    arrayValues.add(obj.toString());
                                }

                            }

                        }
                    }

                    if (isResource) {
                        parameters.addParameter(
                                new Parameters.ParametersParameterComponent().setName(cqlcriteria).setResource(bundle));
                    } else {
                        if (arrayValues.size() > 0) {
                            parameters.addParameter(cqlcriteria, String.join(",", arrayValues));
                        }

                    }

                } else if (cqlResult instanceof Resource) {
                    Resource resultres = (Resource) cqlResult;
                    parameters.addParameter(
                            new Parameters.ParametersParameterComponent().setName(cqlcriteria).setResource(resultres));
                } else if (cqlResult instanceof Boolean) {
                    parameters.addParameter(cqlcriteria, (Boolean) cqlResult);

                } else if (cqlResult instanceof org.opencds.cqf.cql.engine.runtime.Code) {

                    CodeType codeType = new CodeType();
                    org.opencds.cqf.cql.engine.runtime.Code code = (org.opencds.cqf.cql.engine.runtime.Code) cqlResult;
                    codeType.setSystem(code.getSystem());
                    codeType.setValue(code.getCode());

                    Parameters.ParametersParameterComponent pc = new Parameters.ParametersParameterComponent()
                            .setName(cqlcriteria);
                    pc.setValue(codeType);
                    parameters.addParameter(pc);
                    System.out.println("casting to Code" + code.getSystem());
                } // parameters.addParameter(cqlcriteria, (Boolean)cqlResult);
                else if (cqlResult instanceof org.opencds.cqf.cql.engine.runtime.Quantity) {
                    org.opencds.cqf.cql.engine.runtime.Quantity qty = (org.opencds.cqf.cql.engine.runtime.Quantity) cqlResult;
                    Quantity quantity = new Quantity();
                    quantity.setUnit(qty.getUnit());
                    quantity.setValue(qty.getValue());
                    Parameters.ParametersParameterComponent pc = new Parameters.ParametersParameterComponent()
                            .setName(cqlcriteria);
                    pc.setValue(quantity);
                    parameters.addParameter(pc);
                    // System.out.println("casting to Quantity" + quantity);

                } else if (cqlResult instanceof HumanName) {
                    HumanName name = (HumanName) cqlResult;
                    parameters.addParameter(cqlcriteria, name.getNameAsSingleString());
                } else if (cqlResult instanceof List) {
                    // System.out.println("List list type");
                    List resultList = (List) cqlResult;
                    if (resultList.size() > 0) {
                        parameters.addParameter(cqlcriteria, resultList.toString());
                    } else {
                        parameters.addParameter(cqlcriteria, "");
                    }

                }

                else if (cqlResult instanceof CodeType) {
                    CodeType codeType = (CodeType) cqlResult;

                    Parameters.ParametersParameterComponent pc = new Parameters.ParametersParameterComponent()
                            .setName(cqlcriteria);
                    pc.setValue(codeType);
                    parameters.addParameter(pc);
                    // System.out.println("casting to Code" + codeType.getValue());

                } else if (cqlResult instanceof org.opencds.cqf.cql.engine.runtime.Date) {
                    DateType date = new DateType();
                    org.opencds.cqf.cql.engine.runtime.Date cqlDate = (org.opencds.cqf.cql.engine.runtime.Date) cqlResult;
                    date.setValueAsString(cqlDate.toString());
                    Parameters.ParametersParameterComponent pc = new Parameters.ParametersParameterComponent()
                            .setName(cqlcriteria);
                    pc.setValue(date);
                    parameters.addParameter(pc);
                    // System.out.println("casting to Date" + date.asStringValue());

                } else if (cqlResult instanceof org.opencds.cqf.cql.engine.runtime.DateTime) {
                    DateTimeType datetime = new DateTimeType();
                    org.opencds.cqf.cql.engine.runtime.DateTime cqlDate = (org.opencds.cqf.cql.engine.runtime.DateTime) cqlResult;
                    datetime.setValueAsString(cqlDate.toString());
                    Parameters.ParametersParameterComponent pc = new Parameters.ParametersParameterComponent()
                            .setName(cqlcriteria);
                    pc.setValue(datetime);
                    parameters.addParameter(pc);
                    // System.out.println("casting to Datetime " + datetime.asStringValue());

                } else if (cqlResult instanceof DateTimeType) {
                    DateTimeType cqlDate = (DateTimeType) cqlResult;
                    Parameters.ParametersParameterComponent pc = new Parameters.ParametersParameterComponent()
                            .setName(cqlcriteria);
                    pc.setValue(cqlDate);
                    parameters.addParameter(pc);
                    // System.out.println("casting to Datetime " + cqlDate.asStringValue());

                } else if (cqlResult instanceof java.math.BigDecimal) {
                    DecimalType decimal = new DecimalType();
                    java.math.BigDecimal cqlDecimal = (java.math.BigDecimal) cqlResult;
                    decimal.setValue(cqlDecimal);
                    Parameters.ParametersParameterComponent pc = new Parameters.ParametersParameterComponent()
                            .setName(cqlcriteria);
                    pc.setValue(decimal);
                    parameters.addParameter(pc);
                    // System.out.println("casting to decimal" + decimal.asStringValue());

                } else if (cqlResult instanceof org.opencds.cqf.cql.engine.runtime.Interval) {
                    Period cqlPeriod = new Period();
                    // System.out.println("received interval : " + cqlResult.toString());

                    org.opencds.cqf.cql.engine.runtime.Interval cqlInterval = (org.opencds.cqf.cql.engine.runtime.Interval) cqlResult;
                    org.opencds.cqf.cql.engine.runtime.DateTime startTime = (org.opencds.cqf.cql.engine.runtime.DateTime) cqlInterval
                            .getStart();
                    org.opencds.cqf.cql.engine.runtime.DateTime endTime = (org.opencds.cqf.cql.engine.runtime.DateTime) cqlInterval
                            .getEnd();
                    cqlPeriod.setStart(java.util.Date.from(startTime.getDateTime().toInstant()));
                    cqlPeriod.setEnd(java.util.Date.from(endTime.getDateTime().toInstant()));
                    Parameters.ParametersParameterComponent pc = new Parameters.ParametersParameterComponent()
                            .setName(cqlcriteria);
                    pc.setValue(cqlPeriod);
                    parameters.addParameter(pc);
                    // System.out.println("casting to Period " + cqlPeriod.toString());

                } else if (cqlResult instanceof String) {
                    parameters.addParameter(cqlcriteria, (String) cqlResult);

                } else if (cqlResult instanceof Integer) {
                    IntegerType intType = new IntegerType();
                    intType.setValue((Integer) cqlResult);
                    Parameters.ParametersParameterComponent pc = new Parameters.ParametersParameterComponent()
                            .setName(cqlcriteria);
                    pc.setValue(intType);
                    parameters.addParameter(pc);

                } else if (cqlResult instanceof DecimalType) {
                    Parameters.ParametersParameterComponent pc = new Parameters.ParametersParameterComponent()
                            .setName(cqlcriteria);
                    pc.setValue((DecimalType) cqlResult);
                    parameters.addParameter(pc);

                } else if (cqlResult instanceof CodeableConcept) {
                    Parameters.ParametersParameterComponent pc = new Parameters.ParametersParameterComponent()
                            .setName(cqlcriteria);
                    CodeableConcept code = (CodeableConcept)cqlResult;
                    pc.setValue(code);
                    parameters.addParameter(pc);

                }

                else {
                    System.out.println("Final no cast available");
                    System.out.println("String value of result is " + cqlResult.toString());
                    parameters.addParameter(cqlcriteria, "");

                }

            }

        }

        return parameters;
    }

    public Object evaluateCqlExpression(Context context, String patientId, String criteria) {
        context.setContextValue("Patient", patientId);
        // Hack to clear expression cache
        // See cqf-ruler github issue #153
        // try {
        // Field privateField = Context.class.getDeclaredField("expressions");
        // privateField.setAccessible(true);
        // LinkedHashMap<String, Object> expressions = (LinkedHashMap<String, Object>)
        // privateField.get(context);
        // expressions.clear();

        // } catch (Exception e) {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }

        logger.info("Evaluating expression :" + criteria);

        if (context.resolveExpressionRef(criteria) == null) {
            logger.info("resolve expression is null");
        }

        Object result = context.resolveExpressionRef(criteria).evaluate(context);

        return result;

    }

    private Iterable<Resource> evaluateCriteria(Context context, Patient patient,
            Measure.MeasureGroupPopulationComponent pop) {
        if (!pop.hasCriteria()) {
            return Collections.emptyList();
        }
        String expression = pop.getCriteria().getExpression();
        String patientId = patient.getIdElement().getIdPart();

        Object result = evaluateCqlExpression(context, patientId, expression);
        if (result == null) {
            return Collections.emptyList();
        }
        if (result instanceof Boolean) {
            if (((Boolean) result)) {

                return Collections.singletonList(patient);
            } else {
                return Collections.emptyList();
            }
        }

        return (Iterable) result;

    }

    private boolean evaluatePopulationCriteria(Context context, Patient patient,
            Measure.MeasureGroupPopulationComponent criteria, HashMap<String, Resource> population,
            HashMap<String, Patient> populationPatients, Measure.MeasureGroupPopulationComponent exclusionCriteria,
            HashMap<String, Resource> exclusionPopulation, HashMap<String, Patient> exclusionPatients) {
        boolean inPopulation = false;
        if (criteria != null) {

            for (Resource resource : evaluateCriteria(context, patient, criteria)) {
                inPopulation = true;
                population.put(resource.getIdElement().getIdPart(), resource);
            }
        }

        if (inPopulation) {
            // Are they in the exclusion?
            if (exclusionCriteria != null) {
                for (Resource resource : evaluateCriteria(context, patient, exclusionCriteria)) {
                    inPopulation = false;
                    exclusionPopulation.put(resource.getIdElement().getIdPart(), resource);
                    population.remove(resource.getIdElement().getIdPart());
                }
            }
        }

        if (inPopulation && populationPatients != null) {
            populationPatients.put(patient.getIdElement().getIdPart(), patient);
        }
        if (!inPopulation && exclusionPatients != null) {
            exclusionPatients.put(patient.getIdElement().getIdPart(), patient);
        }

        return inPopulation;
    }

    private void addPopulationCriteriaReport(MeasureReport report,
            MeasureReport.MeasureReportGroupComponent reportGroup,
            Measure.MeasureGroupPopulationComponent populationCriteria, int populationCount,
            Iterable<Patient> patientPopulation) {
        if (populationCriteria != null) {
            MeasureReport.MeasureReportGroupPopulationComponent populationReport = new MeasureReport.MeasureReportGroupPopulationComponent();
            populationReport.setCode(populationCriteria.getCode());
            if (report.getType() == MeasureReport.MeasureReportType.SUBJECTLIST && patientPopulation != null) {
                ListResource SUBJECTLIST = new ListResource();
                SUBJECTLIST.setId(UUID.randomUUID().toString());
                populationReport.setSubjectResults(new Reference().setReference("#" + SUBJECTLIST.getId()));
                for (Patient patient : patientPopulation) {
                    ListResource.ListEntryComponent entry = new ListResource.ListEntryComponent()
                            .setItem(new Reference()
                                    .setReference(patient.getIdElement().getIdPart().startsWith("Patient/")
                                            ? patient.getIdElement().getIdPart()
                                            : String.format("Patient/%s", patient.getIdElement().getIdPart()))
                                    .setDisplay(patient.getNameFirstRep().getNameAsSingleString()));
                    SUBJECTLIST.addEntry(entry);
                }
                report.addContained(SUBJECTLIST);
            }
            populationReport.setCount(populationCount);
            reportGroup.addPopulation(populationReport);
        }
    }

    private MeasureReport evaluate(Measure measure, Context context, List<Patient> patients,
            MeasureReport.MeasureReportType type) {

        MeasureReportBuilder reportBuilder = new MeasureReportBuilder();
        reportBuilder.buildStatus("complete");
        reportBuilder.buildType(type);
        reportBuilder.buildMeasureReference(
                measure.getIdElement().getResourceType() + "/" + measure.getIdElement().getIdPart());
        if (type == MeasureReport.MeasureReportType.INDIVIDUAL && !patients.isEmpty()) {
            IdType patientId = patients.get(0).getIdElement();
            reportBuilder.buildPatientReference(patientId.getResourceType() + "/" + patientId.getIdPart());
        }
        reportBuilder.buildPeriod(measurementPeriod);

        MeasureReport report = reportBuilder.build();
        report.setImprovementNotation(measure.getImprovementNotation());
        HashMap<String, Resource> resources = new HashMap<>();
        HashMap<String, HashSet<String>> codeToResourceMap = new HashMap<>();

        MeasureScoring measureScoring = MeasureScoring.fromCode(measure.getScoring().getCodingFirstRep().getCode());
        if (measureScoring == null) {
            throw new RuntimeException("Measure scoring is required in order to calculate.");
        }

        for (Measure.MeasureGroupComponent group : measure.getGroup()) {
            MeasureReport.MeasureReportGroupComponent reportGroup = new MeasureReport.MeasureReportGroupComponent();
            reportGroup.setId(group.getId());
            report.getGroup().add(reportGroup);

            // Declare variables to avoid a hash lookup on every patient
            // TODO: Isn't quite right, there may be multiple initial populations for a
            // ratio measure...
            Measure.MeasureGroupPopulationComponent initialPopulationCriteria = null;
            Measure.MeasureGroupPopulationComponent numeratorCriteria = null;
            Measure.MeasureGroupPopulationComponent numeratorExclusionCriteria = null;
            Measure.MeasureGroupPopulationComponent denominatorCriteria = null;
            Measure.MeasureGroupPopulationComponent denominatorExclusionCriteria = null;
            Measure.MeasureGroupPopulationComponent denominatorExceptionCriteria = null;
            Measure.MeasureGroupPopulationComponent measurePopulationCriteria = null;
            Measure.MeasureGroupPopulationComponent measurePopulationExclusionCriteria = null;
            // TODO: Isn't quite right, there may be multiple measure observations...
            Measure.MeasureGroupPopulationComponent measureObservationCriteria = null;

            HashMap<String, Resource> initialPopulation = null;
            HashMap<String, Resource> numerator = null;
            HashMap<String, Resource> numeratorExclusion = null;
            HashMap<String, Resource> denominator = null;
            HashMap<String, Resource> denominatorExclusion = null;
            HashMap<String, Resource> denominatorException = null;
            HashMap<String, Resource> measurePopulation = null;
            HashMap<String, Resource> measurePopulationExclusion = null;
            HashMap<String, Resource> measureObservation = null;

            HashMap<String, Patient> initialPopulationPatients = null;
            HashMap<String, Patient> numeratorPatients = null;
            HashMap<String, Patient> numeratorExclusionPatients = null;
            HashMap<String, Patient> denominatorPatients = null;
            HashMap<String, Patient> denominatorExclusionPatients = null;
            HashMap<String, Patient> denominatorExceptionPatients = null;
            HashMap<String, Patient> measurePopulationPatients = null;
            HashMap<String, Patient> measurePopulationExclusionPatients = null;

            for (Measure.MeasureGroupPopulationComponent pop : group.getPopulation()) {
                MeasurePopulationType populationType = MeasurePopulationType
                        .fromCode(pop.getCode().getCodingFirstRep().getCode());
                if (populationType != null) {
                    switch (populationType) {
                        case INITIALPOPULATION:
                            initialPopulationCriteria = pop;
                            initialPopulation = new HashMap<>();
                            if (type == MeasureReport.MeasureReportType.SUBJECTLIST) {
                                initialPopulationPatients = new HashMap<>();
                            }
                            break;
                        case NUMERATOR:
                            numeratorCriteria = pop;
                            numerator = new HashMap<>();
                            if (type == MeasureReport.MeasureReportType.SUBJECTLIST) {
                                numeratorPatients = new HashMap<>();
                            }
                            break;
                        case NUMERATOREXCLUSION:
                            numeratorExclusionCriteria = pop;
                            numeratorExclusion = new HashMap<>();
                            if (type == MeasureReport.MeasureReportType.SUBJECTLIST) {
                                numeratorExclusionPatients = new HashMap<>();
                            }
                            break;
                        case DENOMINATOR:
                            denominatorCriteria = pop;
                            denominator = new HashMap<>();
                            if (type == MeasureReport.MeasureReportType.SUBJECTLIST) {
                                denominatorPatients = new HashMap<>();
                            }
                            break;
                        case DENOMINATOREXCLUSION:
                            denominatorExclusionCriteria = pop;
                            denominatorExclusion = new HashMap<>();
                            if (type == MeasureReport.MeasureReportType.SUBJECTLIST) {
                                denominatorExclusionPatients = new HashMap<>();
                            }
                            break;
                        case DENOMINATOREXCEPTION:
                            denominatorExceptionCriteria = pop;
                            denominatorException = new HashMap<>();
                            if (type == MeasureReport.MeasureReportType.SUBJECTLIST) {
                                denominatorExceptionPatients = new HashMap<>();
                            }
                            break;
                        case MEASUREPOPULATION:
                            measurePopulationCriteria = pop;
                            measurePopulation = new HashMap<>();
                            if (type == MeasureReport.MeasureReportType.SUBJECTLIST) {
                                measurePopulationPatients = new HashMap<>();
                            }
                            break;
                        case MEASUREPOPULATIONEXCLUSION:
                            measurePopulationExclusionCriteria = pop;
                            measurePopulationExclusion = new HashMap<>();
                            if (type == MeasureReport.MeasureReportType.SUBJECTLIST) {
                                measurePopulationExclusionPatients = new HashMap<>();
                            }
                            break;
                        case MEASUREOBSERVATION:
                            break;
                    }
                }
            }

            switch (measureScoring) {
                case PROPORTION:
                case RATIO: {

                    // For each patient in the initial population
                    for (Patient patient : patients) {

                        // Are they in the initial population?

                        boolean inInitialPopulation = evaluatePopulationCriteria(context, patient,
                                initialPopulationCriteria, initialPopulation, initialPopulationPatients, null, null,
                                null);
                        populateResourceMap(context, MeasurePopulationType.INITIALPOPULATION, resources,
                                codeToResourceMap);

                        if (inInitialPopulation) {

                            // Are they in the denominator?
                            boolean inDenominator = evaluatePopulationCriteria(context, patient, denominatorCriteria,
                                    denominator, denominatorPatients, denominatorExclusionCriteria,
                                    denominatorExclusion, denominatorExclusionPatients);
                            populateResourceMap(context, MeasurePopulationType.DENOMINATOR, resources,
                                    codeToResourceMap);

                            if (inDenominator) {

                                // Are they in the numerator?
                                boolean inNumerator = evaluatePopulationCriteria(context, patient, numeratorCriteria,
                                        numerator, numeratorPatients, numeratorExclusionCriteria, numeratorExclusion,
                                        numeratorExclusionPatients);
                                populateResourceMap(context, MeasurePopulationType.NUMERATOR, resources,
                                        codeToResourceMap);

                                if (!inNumerator && inDenominator && (denominatorExceptionCriteria != null)) {

                                    // Are they in the denominator exception?
                                    boolean inException = false;
                                    for (Resource resource : evaluateCriteria(context, patient,
                                            denominatorExceptionCriteria)) {
                                        inException = true;
                                        denominatorException.put(resource.getIdElement().getIdPart(), resource);
                                        denominator.remove(resource.getIdElement().getIdPart());
                                        populateResourceMap(context, MeasurePopulationType.DENOMINATOREXCEPTION,
                                                resources, codeToResourceMap);
                                    }

                                    if (inException) {
                                        if (denominatorExceptionPatients != null) {

                                            denominatorExceptionPatients.put(patient.getIdElement().getIdPart(),
                                                    patient);
                                        }
                                        if (denominatorPatients != null) {

                                            denominatorPatients.remove(patient.getIdElement().getIdPart());
                                        }
                                    }
                                }
                            }
                        }

                    }

                    // Calculate actual measure score, Count(numerator) / Count(denominator)
                    if (denominator != null && numerator != null && denominator.size() > 0) {
                        logger.info("calculating measure score");
                        reportGroup.setMeasureScore(new Quantity(numerator.size() / (double) denominator.size()));
                    }

                    break;
                }
                case CONTINUOUSVARIABLE: {

                    // For each patient in the patient list
                    for (Patient patient : patients) {

                        // Are they in the initial population?
                        boolean inInitialPopulation = evaluatePopulationCriteria(context, patient,
                                initialPopulationCriteria, initialPopulation, initialPopulationPatients, null, null,
                                null);
                        populateResourceMap(context, MeasurePopulationType.INITIALPOPULATION, resources,
                                codeToResourceMap);

                        if (inInitialPopulation) {
                            // Are they in the measure population?
                            boolean inMeasurePopulation = evaluatePopulationCriteria(context, patient,
                                    measurePopulationCriteria, measurePopulation, measurePopulationPatients,
                                    measurePopulationExclusionCriteria, measurePopulationExclusion,
                                    measurePopulationExclusionPatients);

                            if (inMeasurePopulation) {
                                // TODO: Evaluate measure observations
                                for (Resource resource : evaluateCriteria(context, patient,
                                        measureObservationCriteria)) {
                                    measureObservation.put(resource.getIdElement().getIdPart(), resource);
                                }
                            }
                        }
                    }

                    break;
                }
                case COHORT: {

                    // For each patient in the patient list
                    for (Patient patient : patients) {
                        // Are they in the initial population?
                        boolean inInitialPopulation = evaluatePopulationCriteria(context, patient,
                                initialPopulationCriteria, initialPopulation, initialPopulationPatients, null, null,
                                null);
                        populateResourceMap(context, MeasurePopulationType.INITIALPOPULATION, resources,
                                codeToResourceMap);
                                
                    }

                    break;
                }
            }

            // Add population reports for each group

            addPopulationCriteriaReport(report, reportGroup, initialPopulationCriteria,
                    initialPopulation != null ? initialPopulation.size() : 0,
                    initialPopulationPatients != null ? initialPopulationPatients.values() : null);
            addPopulationCriteriaReport(report, reportGroup, numeratorCriteria,
                    numerator != null ? numerator.size() : 0,
                    numeratorPatients != null ? numeratorPatients.values() : null);
            addPopulationCriteriaReport(report, reportGroup, numeratorExclusionCriteria,
                    numeratorExclusion != null ? numeratorExclusion.size() : 0,
                    numeratorExclusionPatients != null ? numeratorExclusionPatients.values() : null);
            addPopulationCriteriaReport(report, reportGroup, denominatorCriteria,
                    denominator != null ? denominator.size() : 0,
                    denominatorPatients != null ? denominatorPatients.values() : null);
            addPopulationCriteriaReport(report, reportGroup, denominatorExclusionCriteria,
                    denominatorExclusion != null ? denominatorExclusion.size() : 0,
                    denominatorExclusionPatients != null ? denominatorExclusionPatients.values() : null);
            addPopulationCriteriaReport(report, reportGroup, denominatorExceptionCriteria,
                    denominatorException != null ? denominatorException.size() : 0,
                    denominatorExceptionPatients != null ? denominatorExceptionPatients.values() : null);
            addPopulationCriteriaReport(report, reportGroup, measurePopulationCriteria,
                    measurePopulation != null ? measurePopulation.size() : 0,
                    measurePopulationPatients != null ? measurePopulationPatients.values() : null);
            addPopulationCriteriaReport(report, reportGroup, measurePopulationExclusionCriteria,
                    measurePopulationExclusion != null ? measurePopulationExclusion.size() : 0,
                    measurePopulationExclusionPatients != null ? measurePopulationExclusionPatients.values() : null);

            // TODO: Measure Observations...
        }

        for (String key : codeToResourceMap.keySet()) {
            org.hl7.fhir.r4.model.ListResource list = new org.hl7.fhir.r4.model.ListResource();
            for (String element : codeToResourceMap.get(key)) {
                org.hl7.fhir.r4.model.ListResource.ListEntryComponent comp = new org.hl7.fhir.r4.model.ListResource.ListEntryComponent();
                comp.setItem(new Reference(element));
                list.addEntry(comp);
            }

            if (!list.isEmpty()) {
                list.setId(UUID.randomUUID().toString());
                list.setTitle(key);
                resources.put(list.getId(), list);
            }
        }

        if (!resources.isEmpty()) {
            FhirMeasureBundler bundler = new FhirMeasureBundler();
            org.hl7.fhir.r4.model.Bundle evaluatedResources = bundler.bundle(resources.values());
            evaluatedResources.setId("#"+UUID.randomUUID().toString());
            report.setEvaluatedResource(Collections.singletonList(new Reference(evaluatedResources.getId())));
            report.addContained(evaluatedResources);
            
            // List<Reference> evaluatedResourceIds = new ArrayList<>();
            
            // for (Resource resource: resources.values()){
            //     evaluatedResourceIds.add(new Reference(resource.getId()));
                
            // }
            // report.setEvaluatedResource(evaluatedResourceIds);
            
            
        }

        return report;
    }

    private void populateResourceMap(Context context, MeasurePopulationType type, HashMap<String, Resource> resources,
            HashMap<String, HashSet<String>> codeToResourceMap) {
        if (context.getEvaluatedResources().isEmpty()) {
            return;
        }

        if (!codeToResourceMap.containsKey(type.toCode())) {
            codeToResourceMap.put(type.toCode(), new HashSet<>());
        }

        HashSet<String> codeHashSet = codeToResourceMap.get((type.toCode()));

        for (Object o : context.getEvaluatedResources()) {
            if (o instanceof Resource) {
                Resource r = (Resource) o;
                String id = (r.getIdElement().getResourceType() != null ? (r.getIdElement().getResourceType() + "/")
                        : "") + r.getIdElement().getIdPart();
                if (!codeHashSet.contains(id)) {
                    codeHashSet.add(id);
                }

                if (!resources.containsKey(id)) {
                    resources.put(id, r);
                }
            }
        }

        context.clearEvaluatedResources();
    }
}