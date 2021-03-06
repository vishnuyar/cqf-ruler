package org.opencds.cqf.r4.standalone;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.json.JSONObject;
import org.opencds.cqf.library.r4.NarrativeProvider;
import org.opencds.cqf.r4.evaluation.ProviderFactory;
import org.opencds.cqf.r4.providers.HQMFProvider;
import org.opencds.cqf.r4.providers.JpaTerminologyProvider;
import org.opencds.cqf.r4.providers.MeasureOperationsProvider;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.jpa.api.dao.DaoRegistry;
import ca.uhn.fhir.jpa.rp.r4.LibraryResourceProvider;
import ca.uhn.fhir.jpa.rp.r4.MeasureResourceProvider;
import ca.uhn.fhir.jpa.rp.r4.ValueSetResourceProvider;
import ca.uhn.fhir.jpa.term.TermReadSvcR4;
import ca.uhn.fhir.parser.DataFormatException;
import ca.uhn.fhir.parser.IParser;

/**
 * Hello world!
 */
public final class App {
    

    
    public static void main(String[] args)  {
        Parameters libParameters;
        try {
            App app = new App();
            libParameters = app.getResource();
            MeasureOperationsProvider measureProvider = app.getMeasureOperations();
            String result = app.evaluateLibrary(libParameters, measureProvider);
            JSONObject json = new JSONObject(result); // Convert text to object
            System.out.println(json.toString(4)); 
            //System.out.println("The result recd is: "+result);
            System.exit(0); 
        } catch (DataFormatException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        

    }

    public MeasureOperationsProvider getMeasureOperations() {
        FhirContext fhirContext = FhirContext.forR4();
        //ITermReadSvcR4 terminologySvcR4, FhirContext context, ValueSetResourceProvider valueSetResourceProvider
        TermReadSvcR4 terminologySvcR4 = new TermReadSvcR4();
        ValueSetResourceProvider valueSetResourceProvider = new ValueSetResourceProvider();
        JpaTerminologyProvider defaultTerminologyProvider = new JpaTerminologyProvider(terminologySvcR4,fhirContext,valueSetResourceProvider);
        DaoRegistry registry = new DaoRegistry();
        ProviderFactory factory = new ProviderFactory(fhirContext, registry, defaultTerminologyProvider);
        NarrativeProvider narrativeProvider = new NarrativeProvider();
        HQMFProvider hqmfProvider = new HQMFProvider();
        LibraryResourceProvider libraryResourceProvider = new LibraryResourceProvider();
        MeasureResourceProvider measureResourceProvider = new MeasureResourceProvider();

        MeasureOperationsProvider measureProvider = new MeasureOperationsProvider(registry, factory, narrativeProvider,
                hqmfProvider, libraryResourceProvider, measureResourceProvider);
        return measureProvider;
    }

    public String evaluateLibrary(Parameters libParameters, MeasureOperationsProvider measureProvider) {
        String libraryId,criteria, subject, periodStart, periodEnd, source, patientServerUrl, patientServerToken, libraryServerUrl, libraryServerToken,criteriaList,user,pass;
        libraryId =criteria =subject= periodStart=periodEnd= source= patientServerUrl= patientServerToken=libraryServerUrl= libraryServerToken= criteriaList=user=pass = null;
        Bundle valueSetsBundle, dataBundle, libBundle;
        valueSetsBundle =dataBundle = libBundle= null;
        Parameters parameters = null;
        for (ParametersParameterComponent para:libParameters.getParameter()){
            // System.out.println("paraname: "+para.getName());
            // System.out.println("paratype: "+para.fhirType());
            // System.out.println("parresource: "+FhirContext.forR4().newJsonParser().encodeResourceToString(para.getResource()));
            if (para.getName().equals("libraryId")){
                libraryId =  para.getValue().toString();
                System.out.println(libraryId);
            }
            if (para.getName().equals("criteria")){
                criteria = para.getValue().toString();
            }
            if (para.getName().equals("subject")){
                subject = para.getValue().toString();
            }
            if (para.getName().equals("periodStart")){
                periodStart = para.getValue().toString();
            }
            if (para.getName().equals("periodEnd")){
                periodEnd = para.getValue().toString();
            }
            if (para.getName().equals("patientServerUrl")){
                patientServerUrl = para.getValue().toString();
            }
            if (para.getName().equals("patientServerToken")){
                patientServerToken = para.getValue().toString();
            }
            if (para.getName().equals("libraryServerUrl")){
                libraryServerUrl = para.getValue().toString();
            }
            if (para.getName().equals("libraryServerToken")){
                libraryServerToken = para.getValue().toString();
            }
            if (para.getName().equals("criteriaList")){
                criteriaList = para.getValue().toString();
            }
            if (para.getName().equals("valueSetsBundle")){
                valueSetsBundle = (Bundle)para.getResource();
            }
            if (para.getName().equals("dataBundle")){
                dataBundle = (Bundle)para.getResource();
            }
            if (para.getName().equals("libBundle")){
                libBundle = (Bundle)para.getResource();
            }
            if (para.getName().equals("parameters")){
                parameters = (Parameters)para.getResource();
            }
        }
        Parameters result =  measureProvider.libraryEvaluate(libraryId, criteria, subject, periodStart, periodEnd, source, patientServerUrl, patientServerToken, libraryServerUrl, libraryServerToken, criteriaList, valueSetsBundle, dataBundle, libBundle, user, parameters, pass);
        return FhirContext.forR4().newJsonParser().encodeResourceToString(result);
    }

    public Parameters getResource() {
        File parafile = new File("/Users/sreekanth/githubrepos/Libevaluate-tool/remotelib.json");
        IParser parser = FhirContext.forR4().newJsonParser();
        Parameters response;
        try {
            response = parser.parseResource(Parameters.class, new FileReader(parafile));
            return response;
        } catch (DataFormatException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
        
    }

    

}
