package org.opencds.cqf.r4.servlet;

import java.io.IOException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.ActivityDefinition;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Claim;
import org.hl7.fhir.r4.model.CodeSystem;
import org.hl7.fhir.r4.model.Coverage;
import org.hl7.fhir.r4.model.Endpoint;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.Measure;
import org.hl7.fhir.r4.model.Meta;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.PlanDefinition;
import org.hl7.fhir.r4.model.ValueSet;
import org.opencds.cqf.common.config.HapiProperties;
import org.opencds.cqf.common.evaluation.EvaluationProviderFactory;
import org.opencds.cqf.common.retrieve.JpaFhirRetrieveProvider;
import org.opencds.cqf.common.retrieve.RemoteRetrieveProvider;
import org.opencds.cqf.cql.engine.fhir.searchparam.SearchParameterResolver;
import org.opencds.cqf.library.r4.NarrativeProvider;
import org.opencds.cqf.measure.r4.CodeTerminologyRef;
import org.opencds.cqf.measure.r4.CqfMeasure;
import org.opencds.cqf.measure.r4.PopulationCriteriaMap;
import org.opencds.cqf.measure.r4.VersionedTerminologyRef;
import org.opencds.cqf.r4.evaluation.ProviderFactory;
import org.opencds.cqf.r4.providers.ActivityDefinitionApplyProvider;
import org.opencds.cqf.r4.providers.ApplyCqlOperationProvider;
import org.opencds.cqf.r4.providers.CacheValueSetsProvider;
import org.opencds.cqf.r4.providers.ClaimProvider;
import org.opencds.cqf.r4.providers.CodeSystemUpdateProvider;
import org.opencds.cqf.r4.providers.CqlExecutionProvider;
import org.opencds.cqf.r4.providers.HQMFProvider;
import org.opencds.cqf.r4.providers.JpaTerminologyProvider;
import org.opencds.cqf.r4.providers.LibraryOperationsProvider;
import org.opencds.cqf.r4.providers.MeasureOperationsProvider;
import org.opencds.cqf.r4.providers.PatientProvider;
import org.opencds.cqf.r4.providers.PlanDefinitionApplyProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.web.cors.CorsConfiguration;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.jpa.api.config.DaoConfig;
import ca.uhn.fhir.jpa.api.dao.DaoRegistry;
import ca.uhn.fhir.jpa.api.dao.IFhirResourceDao;
import ca.uhn.fhir.jpa.api.dao.IFhirResourceDaoPatient;
import ca.uhn.fhir.jpa.api.dao.IFhirSystemDao;
import ca.uhn.fhir.rest.server.provider.ResourceProviderFactory;
import ca.uhn.fhir.jpa.provider.BaseJpaResourceProvider;
import ca.uhn.fhir.jpa.provider.TerminologyUploaderProvider;
import ca.uhn.fhir.jpa.provider.r4.JpaConformanceProviderR4;
import ca.uhn.fhir.jpa.provider.r4.JpaSystemProviderR4;
import ca.uhn.fhir.jpa.rp.r4.LibraryResourceProvider;
import ca.uhn.fhir.jpa.rp.r4.MeasureResourceProvider;
import ca.uhn.fhir.jpa.rp.r4.ValueSetResourceProvider;
import ca.uhn.fhir.jpa.search.DatabaseBackedPagingProvider;
import ca.uhn.fhir.jpa.searchparam.registry.SearchParamRegistryImpl;
import ca.uhn.fhir.jpa.term.api.ITermReadSvcR4;
import ca.uhn.fhir.narrative.DefaultThymeleafNarrativeGenerator;
import ca.uhn.fhir.rest.client.api.ServerValidationModeEnum;
import ca.uhn.fhir.rest.server.HardcodedServerAddressStrategy;
import ca.uhn.fhir.rest.server.RestfulServer;
import ca.uhn.fhir.rest.server.interceptor.CorsInterceptor;
import ca.uhn.fhir.rest.server.interceptor.ResponseHighlighterInterceptor;

public class BaseServlet extends RestfulServer {
    DaoRegistry registry;
    FhirContext fhirContext;
    ApplicationContext appCtx;

    @SuppressWarnings("unchecked")
    @Override
    protected void initialize() throws ServletException {
        super.initialize();

        // System level providers
        appCtx = (ApplicationContext) getServletContext()
                .getAttribute("org.springframework.web.context.WebApplicationContext.ROOT");

        // Fhir Context
        this.fhirContext = appCtx.getBean(FhirContext.class);
        this.fhirContext.getRestfulClientFactory().setServerValidationMode(ServerValidationModeEnum.NEVER);
        this.fhirContext.registerCustomType(VersionedTerminologyRef.class);
        this.fhirContext.registerCustomType(CodeTerminologyRef.class);
        this.fhirContext.registerCustomType(PopulationCriteriaMap.class);
        this.fhirContext.registerCustomType(CqfMeasure.class);
        setFhirContext(this.fhirContext);

        // System and Resource Daos
        IFhirSystemDao<Bundle, Meta> systemDao = appCtx.getBean("mySystemDaoR4", IFhirSystemDao.class);
        this.registry = appCtx.getBean(DaoRegistry.class);

        // System and Resource Providers
        Object systemProvider = appCtx.getBean("mySystemProviderR4", JpaSystemProviderR4.class);
        registerProvider(systemProvider);

        ResourceProviderFactory resourceProviders = appCtx.getBean("myResourceProvidersR4",
                ResourceProviderFactory.class);
        registerProviders(resourceProviders.createProviders());

        JpaConformanceProviderR4 confProvider = new JpaConformanceProviderR4(this, systemDao,
                appCtx.getBean(DaoConfig.class), new SearchParamRegistryImpl());
        confProvider.setImplementationDescription("CQF Ruler FHIR R4 Server");
        setServerConformanceProvider(confProvider);

        JpaTerminologyProvider localSystemTerminologyProvider = new JpaTerminologyProvider(
                appCtx.getBean("terminologyService", ITermReadSvcR4.class), getFhirContext(),
                (ValueSetResourceProvider) this.getResourceProvider(ValueSet.class));
        EvaluationProviderFactory providerFactory = new ProviderFactory(this.fhirContext, this.registry,
                localSystemTerminologyProvider);

        resolveProviders(providerFactory, localSystemTerminologyProvider, this.registry);

        // CdsHooksServlet.provider = provider;

        /*
         * ETag Support
         */
        setETagSupport(HapiProperties.getEtagSupport());

        /*
         * This server tries to dynamically generate narratives
         */
        FhirContext ctx = getFhirContext();
        ctx.setNarrativeGenerator(new DefaultThymeleafNarrativeGenerator());

        /*
         * Default to JSON and pretty printing
         */
        setDefaultPrettyPrint(HapiProperties.getDefaultPrettyPrint());

        /*
         * Default encoding
         */
        setDefaultResponseEncoding(HapiProperties.getDefaultEncoding());

        /*
         * This configures the server to page search results to and from the database,
         * instead of only paging them to memory. This may mean a performance hit when
         * performing searches that return lots of results, but makes the server much
         * more scalable.
         */
        setPagingProvider(appCtx.getBean(DatabaseBackedPagingProvider.class));

        /*
         * This interceptor formats the output using nice colourful HTML output when the
         * request is detected to come from a browser.
         */
        ResponseHighlighterInterceptor responseHighlighterInterceptor = appCtx
                .getBean(ResponseHighlighterInterceptor.class);
        this.registerInterceptor(responseHighlighterInterceptor);

        /*
         * If you are hosting this server at a specific DNS name, the server will try to
         * figure out the FHIR base URL based on what the web container tells it, but
         * this doesn't always work. If you are setting links in your search bundles
         * that just refer to "localhost", you might want to use a server address
         * strategy:
         */
        String serverAddress = HapiProperties.getServerAddress();
        if (serverAddress != null && serverAddress.length() > 0) {
            setServerAddressStrategy(new HardcodedServerAddressStrategy(serverAddress));
        }

        registerProvider(appCtx.getBean(TerminologyUploaderProvider.class));

        if (HapiProperties.getCorsEnabled()) {
            CorsConfiguration config = new CorsConfiguration();
            config.addAllowedHeader("x-fhir-starter");
            config.addAllowedHeader("Origin");
            config.addAllowedHeader("Accept");
            config.addAllowedHeader("X-Requested-With");
            config.addAllowedHeader("Content-Type");
            config.addAllowedHeader("patient_server_url");
            config.addAllowedHeader("patient_server_token");
            config.addAllowedHeader("Authorization");
            config.addAllowedHeader("Cache-Control");

            config.addAllowedOrigin(HapiProperties.getCorsAllowedOrigin());

            config.addExposedHeader("Location");
            config.addExposedHeader("Content-Location");
            config.setAllowedMethods(Arrays.asList("GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"));

            // Create the interceptor and register it
            CorsInterceptor interceptor = new CorsInterceptor(config);
            registerInterceptor(interceptor);
        }
    }

    protected NarrativeProvider getNarrativeProvider() {
        return new NarrativeProvider();
    }

    // Since resource provider resolution not lazy, the providers here must be
    // resolved in the correct
    // order of dependencies.
    private void resolveProviders(EvaluationProviderFactory providerFactory,
            JpaTerminologyProvider localSystemTerminologyProvider, DaoRegistry registry) throws ServletException {
        NarrativeProvider narrativeProvider = this.getNarrativeProvider();
        HQMFProvider hqmfProvider = new HQMFProvider();

        // Code System Update
        CodeSystemUpdateProvider csUpdate = new CodeSystemUpdateProvider(this.getDao(ValueSet.class),
                this.getDao(CodeSystem.class));
        this.registerProvider(csUpdate);

        // Cache Value Sets
        CacheValueSetsProvider cvs = new CacheValueSetsProvider(this.registry.getSystemDao(),
                this.getDao(Endpoint.class));
        this.registerProvider(cvs);

        // Library processing
        LibraryOperationsProvider libraryProvider = new LibraryOperationsProvider(
                (LibraryResourceProvider) this.getResourceProvider(Library.class), narrativeProvider);
        this.registerProvider(libraryProvider);

        // CQL Execution
        CqlExecutionProvider cql = new CqlExecutionProvider(libraryProvider, providerFactory);
        this.registerProvider(cql);

        // Bundle processing
        ApplyCqlOperationProvider bundleProvider = new ApplyCqlOperationProvider(providerFactory,
                this.getDao(Bundle.class));
        this.registerProvider(bundleProvider);

        // Measure processing
        MeasureOperationsProvider measureProvider = new MeasureOperationsProvider(this.registry, providerFactory,
                narrativeProvider, hqmfProvider, (LibraryResourceProvider) this.getResourceProvider(Library.class),
                (MeasureResourceProvider) this.getResourceProvider(Measure.class));
        this.registerProvider(measureProvider);

        IFhirResourceDao<Patient> patientDao = (IFhirResourceDao<Patient>) appCtx
                .getBean("myPatientDaoR4",
                        IFhirResourceDaoPatient.class);
        IFhirResourceDao<Coverage> coverageDao  = (IFhirResourceDao<Coverage>) appCtx
                .getBean("myCoverageDaoR4", ca.uhn.fhir.jpa.api.dao.IFhirResourceDao.class);

        PatientProvider patientRp = new PatientProvider(this.registry.getSystemDao(),coverageDao);
        patientRp.setDao(patientDao);
    	  registerProvider(patientRp);

        IFhirResourceDao<Claim> claimDao = (IFhirResourceDao<Claim>) appCtx.getBean("myClaimDaoR4", IFhirResourceDao.class);
        ClaimProvider claimRp = new ClaimProvider(appCtx);
        claimRp.setDao(claimDao);
    	  registerProvider(claimRp);
    	

        // // ActivityDefinition processing
        ActivityDefinitionApplyProvider actDefProvider = new ActivityDefinitionApplyProvider(this.fhirContext, cql, this.getDao(ActivityDefinition.class));
        this.registerProvider(actDefProvider);

        JpaFhirRetrieveProvider localSystemRetrieveProvider = new JpaFhirRetrieveProvider(registry, new SearchParameterResolver(this.fhirContext));

        // PlanDefinition processing
        PlanDefinitionApplyProvider planDefProvider = new PlanDefinitionApplyProvider(this.fhirContext, actDefProvider, this.getDao(PlanDefinition.class), this.getDao(ActivityDefinition.class), cql);
        this.registerProvider(planDefProvider);
        

    }

    protected <T extends IBaseResource> IFhirResourceDao<T> getDao(Class<T> clazz) {
        return this.registry.getResourceDao(clazz);
    }


    protected <T extends IBaseResource> BaseJpaResourceProvider<T>  getResourceProvider(Class<T> clazz) {
        return (BaseJpaResourceProvider<T> ) this.getResourceProviders().stream()
        .filter(x -> x.getResourceType().getSimpleName().equals(clazz.getSimpleName())).findFirst().get();
    }

	// @Override
	// protected void service(HttpServletRequest theReq, HttpServletResponse theResp)
	// 		throws ServletException, IOException {
    //     // TODO Auto-generated method stub
    //     HashMap<String,String> nonLocal = new HashMap<>();
	// 	if (theReq.getHeader("patient_server_url") != null) {
    //         System.out.println("patient_server_url:"+theReq.getHeader("patient_server_url")); 
    //         nonLocal.put("patient_server_url", theReq.getHeader("patient_server_url"));
			
    //     }
    //     if (theReq.getHeader("patient_server_token") != null) {
    //         System.out.println("patient_server_token:"+theReq.getHeader("patient_server_token"));
	// 		nonLocal.put("patient_server_token", theReq.getHeader("patient_server_token"));
    //     }
    //     RemoteRetrieveProvider.patient_fhir.set(nonLocal);
	// 	super.service(theReq, theResp);
	// }
    
    
}
