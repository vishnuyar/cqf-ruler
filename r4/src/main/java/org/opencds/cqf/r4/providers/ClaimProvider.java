package org.opencds.cqf.r4.providers;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Claim;
import org.hl7.fhir.r4.model.ClaimResponse;
import org.hl7.fhir.r4.model.ClaimResponse.ErrorComponent;
import org.hl7.fhir.r4.model.ClaimResponse.Use;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Reference;

import ca.uhn.fhir.jpa.dao.DaoMethodOutcome;
import ca.uhn.fhir.jpa.dao.IFhirResourceDao;
import ca.uhn.fhir.jpa.rp.r4.ClaimResourceProvider;
import ca.uhn.fhir.jpa.rp.r4.PatientResourceProvider;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.annotation.OptionalParam;
import ca.uhn.fhir.rest.annotation.RequiredParam;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import org.springframework.context.ApplicationContext;


import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.Parameters;
import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.opencds.cqf.common.config.HapiProperties;

public class ClaimProvider extends ClaimResourceProvider{
	
	IFhirResourceDao<Bundle> bundleDao;
	IFhirResourceDao<ClaimResponse> ClaimResponseDao;
	JSONArray errors = new JSONArray();
	public ClaimProvider(ApplicationContext appCtx){
		this.bundleDao = (IFhirResourceDao<Bundle>) appCtx.getBean("myBundleDaoR4", IFhirResourceDao.class);
		this.ClaimResponseDao = (IFhirResourceDao<ClaimResponse>) appCtx.getBean("myClaimResponseDaoR4", IFhirResourceDao.class);
		System.out.println("----\n---");
		System.out.println(this.ClaimResponseDao);
		System.out.println(this.bundleDao);
	}
	
	@Override
	public Class<Claim> getResourceType() {
	      return Claim.class;
	}
	
	
	public String getSaltString() {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < 16) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;
    }
	
	
	private String submitX12(String x12_generated,ClaimResponse claimResponse,String bundleJson) {
		String str_result = "";
		try {
            // POST call for token
			StringBuilder sb = new StringBuilder();
			
            URL url = new URL(HapiProperties.getProperty("x12_check_url"));
            byte[] postDataBytes = x12_generated.getBytes("UTF-8");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestProperty("x-api-key", "CXES5OZC0S5RUGMphrOpB75wQjk1ZiGD48OWlTDD");
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "text/plain");
            conn.setRequestProperty("Accept", "text/plain");
            conn.setDoOutput(true);
            conn.getOutputStream().write(postDataBytes);
            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
            String line = null;
            while ((line = in.readLine()) != null) {
                sb.append(line);
            }
            str_result = sb.toString();
          System.out.println("\n  >>> Str/ Res 1:"+str_result);
            if (str_result.contains("HCR*A1")) {
            	claimResponse.setStatus(ClaimResponse.ClaimResponseStatus.ACTIVE);
            	claimResponse.setOutcome(ClaimResponse.RemittanceOutcome.COMPLETE);
            } else if (str_result.contains("HCR*A2")) {
            	claimResponse.setStatus(ClaimResponse.ClaimResponseStatus.ACTIVE);
            	claimResponse.setOutcome(ClaimResponse.RemittanceOutcome.PARTIAL);
            } else if (str_result.contains("HCR*A3")) {
            	claimResponse.setStatus(ClaimResponse.ClaimResponseStatus.ENTEREDINERROR);
            	claimResponse.setOutcome(ClaimResponse.RemittanceOutcome.ERROR);
            } else if (str_result.contains("HCR*A4")) {
            	claimResponse.setStatus(ClaimResponse.ClaimResponseStatus.ACTIVE);
            	claimResponse.setOutcome(ClaimResponse.RemittanceOutcome.QUEUED);
            } else if (str_result.contains("HCR*C")) {
            	claimResponse.setStatus(ClaimResponse.ClaimResponseStatus.CANCELLED);
            	claimResponse.setOutcome(ClaimResponse.RemittanceOutcome.COMPLETE);
            }
            //}
		}
		catch(Exception ex) {
			
		}
		return  str_result;
	}
	
	private String generateX12(String jsonStr,Bundle bundle) {
		String x12_generated = "";
		try {
//	            System.out.println("JSON:\n" + jsonStr);
	            StringBuilder sb = new StringBuilder();
	            String str_result = "";
	            URL url = new URL(HapiProperties.getProperty("x12_generator_url"));
	            byte[] postDataBytes = jsonStr.getBytes("UTF-8");
	            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
	            conn.setRequestProperty("x-api-key", "CXES5OZC0S5RUGMphrOpB75wQjk1ZiGD48OWlTDD");
	            conn.setRequestMethod("POST");
	            conn.setRequestProperty("Content-Type", "application/json");
	            conn.setRequestProperty("Accept", "application/json");
	            conn.setDoOutput(true);
	            conn.getOutputStream().write(postDataBytes);
	            
	            if(conn.getResponseCode() == 200) {
	            	BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
	            	String line = null;
		            while ((line = in.readLine()) != null) {
		                sb.append(line);
		            }
	            }
	            else {
	            	BufferedReader in = new BufferedReader(new InputStreamReader(conn.getErrorStream(), "UTF-8"));
	            	String line = null;
		            while ((line = in.readLine()) != null) {
		                sb.append(line);
		            }
	            }
	            
	            
	            System.out.println("RES:"+sb.toString());
	            JSONObject response = new JSONObject(sb.toString());
	            if(response.get("status").equals("success")) {
	            	x12_generated = response.getString("data");
	            }
	            else {
	            	this.errors = response.getJSONArray("errors");
	            	System.out.println("Errors:"+errors);
	            }
	            System.out.println("\n x12_generated :"+x12_generated);
	        }
		    catch (Exception ex) {
		        ex.printStackTrace();
		    }
		 return x12_generated;
	}
	
	private ClaimResponse  generateClaimResponse(Patient patient , Claim claim ) {
		ClaimResponse retVal = new ClaimResponse();
		try {
			if(patient.getIdentifier().size()>0) {
				String patientIdentifier = patient.getIdentifier().get(0).getValue();
				Reference patientRef = new Reference();
			     if (!patientIdentifier.isEmpty()) {
	                Identifier patientIdentifierObj = new Identifier();
	                patientIdentifierObj.setValue(patientIdentifier);
	                patientRef.setIdentifier(patientIdentifierObj);
	                retVal.setPatient(patientRef);
	             }
			}
			
			if(claim.getIdentifier().size() > 0) {
				 String claimIdentifier = claim.getIdentifier().get(0).getValue();
				 Reference reqRef = new Reference();
		         if (!claimIdentifier.isEmpty()) {
		             Identifier claimIdentifierObj = new Identifier();
		             claimIdentifierObj.setValue(claimIdentifier);
		             reqRef.setIdentifier(claimIdentifierObj);
		         }
		         retVal.setRequest(reqRef);
			 }
		     retVal.setStatus(ClaimResponse.ClaimResponseStatus.ACTIVE);
		     retVal.setOutcome(ClaimResponse.RemittanceOutcome.QUEUED);
		     /*
		     if(claim.getInsurer() != null) {
		    	 if(claim.getInsurer().getIdentifier() !=null) {
		    		 retVal.setInsurer(new Reference().setIdentifier(claim.getInsurer().getIdentifier()));
		    	 }
//		    	 retVal.setInsurer(claim.getInsurer());
		     }
		     if(claim.getProvider()!= null) {
		    	 retVal.setRequestor(new Reference().setIdentifier(claim.getProvider().getIdentifier()));
//		    	 retVal.setRequestor(claim.getProvider());
		     }
		     
		     */
		     
		     retVal.setUse(ClaimResponse.Use.PREAUTHORIZATION );
//		     retVal.setPatient(new Reference(patient.getId()));
		     retVal.setType(claim.getType());
		     if(claim.getSubType() != null) {
		    	 retVal.setSubType(claim.getSubType());
		     }
	         retVal.setCreated(new Date());
	         retVal.setUse(ClaimResponse.Use.PREAUTHORIZATION);

	         
	         retVal.setPreAuthRef(getSaltString());
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
        return retVal;
	} 

	
	  // @Create
    @Operation(name = "$submit", idempotent = true)
    public Bundle claimSubmit(RequestDetails details,
            @OperationParam(name = "claim", min = 1, max = 1, type = Bundle.class) Bundle bundle)
            throws RuntimeException {
    	this.errors = new JSONArray();
        Bundle collectionBundle = new Bundle().setType(Bundle.BundleType.COLLECTION);
        Bundle responseBundle = new Bundle();
        Bundle createdBundle = new Bundle();
        String claimURL = "";
        String patientId = "";
        String claim_response_status = "";
        String claim_response_outcome = "";
        String patientIdentifier = "";
        String claimIdentifier = "";
        Claim claim = new Claim();
        Patient patient = new Patient();
        for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
            collectionBundle.addEntry(entry);
//            System.out.println("ResType : " + entry.getResource().getResourceType());
            if (entry.getResource().getResourceType().toString().equals("Claim")) {
                try {
                    claim = (Claim) entry.getResource();
                    
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else if (entry.getResource().getResourceType().toString().equals("Patient")) {
                try {
                    patient = (Patient) entry.getResource();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        try {

	    	ClaimResponse retVal = generateClaimResponse(patient, claim);
	    	IParser jsonParser = details.getFhirContext().newJsonParser();
            String jsonStr = jsonParser.encodeResourceToString(bundle);
	    	String x12_generated =  generateX12( jsonStr, bundle);
	    	
	    	
	    	if(this.errors.length() > 0) {
	    		retVal.setStatus(ClaimResponse.ClaimResponseStatus.ENTEREDINERROR);
	    		retVal.setOutcome(ClaimResponse.RemittanceOutcome.ERROR);
	    		List<ErrorComponent> errorList= new ArrayList<ErrorComponent>();
	    		for(int i=0;i<this.errors.length();i++) {
	    			JSONObject errorObj = new JSONObject(this.errors.get(i).toString());
	    			System.out.println("errorObj: "+errorObj);
	    			String code = errorObj.getString("code");
	    			String message = errorObj.getString("message");
	    			errorList.add(this.generateErrorComponent(code,message));	    			
	    			
	    		}
	    		retVal.setError(errorList);
	    	}
	    	else {
	    		String x12_response = submitX12( x12_generated, retVal, jsonStr);
		    	retVal = updateClaimResponse(retVal,x12_response);
	            System.out.println("----------X12 Generated--------- \n");
	            System.out.println(x12_response);
	            System.out.println("\n------------------- \n");
	    	}
	    	
            DaoMethodOutcome claimResponseOutcome = ClaimResponseDao.create(retVal);
            ClaimResponse claimResponse = (ClaimResponse) claimResponseOutcome.getResource();
            Bundle.BundleEntryComponent transactionEntry = new Bundle.BundleEntryComponent().setResource(claimResponse);
            responseBundle.addEntry(transactionEntry);
            DaoMethodOutcome bundleOutcome = this.bundleDao.create(collectionBundle);
            createdBundle = (Bundle) bundleOutcome.getResource();
            for (Bundle.BundleEntryComponent entry : createdBundle.getEntry()) {
                responseBundle.addEntry(entry);
            }
            responseBundle.setId(createdBundle.getId());
            responseBundle.setType(Bundle.BundleType.COLLECTION);
            return responseBundle;


        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex.getLocalizedMessage());
        }
    }
	
    private ErrorComponent generateErrorComponent(String code,String message) {
    	ErrorComponent errorComponent = new ErrorComponent();
		CodeableConcept errorCode = new CodeableConcept();
		errorCode.addCoding(new Coding().setDisplay(message).setCode(code));
		errorComponent.setCode(errorCode);
		return errorComponent;
    }
    
	private ClaimResponse updateClaimResponse(ClaimResponse claimResponse,String x12) {
		try {
			
			
			if(x12.toLowerCase().contains("error")) {
				claimResponse.setStatus(ClaimResponse.ClaimResponseStatus.ENTEREDINERROR);
            	claimResponse.setOutcome(ClaimResponse.RemittanceOutcome.ERROR);
				ArrayList<ErrorComponent> ErrList = new ArrayList<ErrorComponent>();
				ErrList.add(this.generateErrorComponent("400", x12));
				System.out.println(ErrList.size());
				claimResponse.setError(ErrList);
				System.out.println(claimResponse.getError().size());
//				BufferedReader bufReader = new BufferedReader(new StringReader(x12));
//				String line="";
//				while( (line=bufReader.readLine()) != null )
//				{
//					System.out.println("line  "+line);
//					if(line.contains("*999*")) {
//						System.out.println("-has error");
//						
//					}
//				}
			}
			else {
				if (x12.contains("HCR*A1")) {
	            	claimResponse.setStatus(ClaimResponse.ClaimResponseStatus.ACTIVE);
	            	claimResponse.setOutcome(ClaimResponse.RemittanceOutcome.COMPLETE);
	       
	            } else if (x12.contains("HCR*A2")) {
	            	claimResponse.setStatus(ClaimResponse.ClaimResponseStatus.ACTIVE);
	            	claimResponse.setOutcome(ClaimResponse.RemittanceOutcome.PARTIAL);
	   
	            } else if (x12.contains("HCR*A3")) {
	            	claimResponse.setStatus(ClaimResponse.ClaimResponseStatus.ENTEREDINERROR);
	            	claimResponse.setOutcome(ClaimResponse.RemittanceOutcome.ERROR);
	      
	            } else if (x12.contains("HCR*A4")) {
	            	claimResponse.setStatus(ClaimResponse.ClaimResponseStatus.ACTIVE);
	            	claimResponse.setOutcome(ClaimResponse.RemittanceOutcome.QUEUED);
	      
	            } else if (x12.contains("HCR*C")) {
	            	claimResponse.setStatus(ClaimResponse.ClaimResponseStatus.CANCELLED);
	            	claimResponse.setOutcome(ClaimResponse.RemittanceOutcome.COMPLETE);
	        
	            }
			}
			
			
			
		}
		catch(Exception ex) {
			
		}
		return claimResponse;
	}

	@Operation(name = "$build", idempotent = true)
	public  Bundle build(RequestDetails theRequestDetails,
			@OperationParam(name = "bundle", min = 1, max = 1, type =  Bundle.class) Bundle bundle) {
//		System.out.println("\n\n op call ssssss"+this.getDao());
		
		return bundle;
		
	}

	
}