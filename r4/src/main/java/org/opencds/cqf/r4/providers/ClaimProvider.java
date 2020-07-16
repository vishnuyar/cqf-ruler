package org.opencds.cqf.r4.providers;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
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
import org.hl7.fhir.r4.model.Resource;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.jpa.dao.DaoMethodOutcome;
import ca.uhn.fhir.jpa.dao.DaoRegistry;
import ca.uhn.fhir.jpa.dao.IFhirResourceDao;
import ca.uhn.fhir.jpa.rp.r4.ClaimResourceProvider;
import ca.uhn.fhir.jpa.rp.r4.PatientResourceProvider;
import ca.uhn.fhir.jpa.searchparam.SearchParameterMap;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.annotation.OptionalParam;
import ca.uhn.fhir.rest.annotation.RequiredParam;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.param.TokenParam;

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
	DaoRegistry registry;
	JSONArray errors = new JSONArray();
	public ClaimProvider(ApplicationContext appCtx){
		this.bundleDao = (IFhirResourceDao<Bundle>) appCtx.getBean("myBundleDaoR4", IFhirResourceDao.class);
		this.ClaimResponseDao = (IFhirResourceDao<ClaimResponse>) appCtx.getBean("myClaimResponseDaoR4", IFhirResourceDao.class);
		this.registry = appCtx.getBean(DaoRegistry.class);
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
	
	
	private String submitX12(String x12_generated,ClaimResponse claimResponse, String strUrl) {
		String str_result = "";
		try {
            // POST call for token
			StringBuilder sb = new StringBuilder();
			
            URL url = new URL(strUrl);
            byte[] postDataBytes = x12_generated.getBytes("UTF-8");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            //conn.setRequestProperty("x-api-key",HapiProperties.getProperty("x12_generator_key") );
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
          System.out.println("\n  X12 Submission reponse :\n"+str_result);
            
            //}
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
		return  str_result;
	}
	
	private String generateX12(String jsonStr) {
		String x12_generated = "";
		try {
//	            System.out.println("JSON:\n" + jsonStr);
				
	            StringBuilder sb = new StringBuilder();
	            String str_result = "";
	            URL url = new URL(HapiProperties.getProperty("x12_generator_url"));
	            byte[] postDataBytes = jsonStr.getBytes("UTF-8");
	            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
	            conn.setRequestProperty("x-api-key", HapiProperties.getProperty("x12_generator_key"));
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
		            x12_generated = sb.toString();
	            }
	            else {
	            	BufferedReader in = new BufferedReader(new InputStreamReader(conn.getErrorStream(), "UTF-8"));
	            	String line = null;
		            while ((line = in.readLine()) != null) {
		                sb.append(line);
		            }
		            System.out.println(sb.toString());
		            JSONObject response = new JSONObject(sb.toString());
		            if(response.has("errors")) {
		            	this.errors = response.getJSONArray("errors");
		            }
		            
	            }	           
	            System.out.println("Errors:"+errors);
	     
	            System.out.println("\n x12_generated :"+x12_generated);
	        }
		    catch (Exception ex) {
		        ex.printStackTrace();
		    }
		 return x12_generated;
	}
	
	private  ClaimResponse readX12Response(String x12Str) {
		ClaimResponse response =null;
		try 
		{

				
	            StringBuilder sb = new StringBuilder();
	            URL url = new URL(HapiProperties.getProperty("x12_reader_url"));
	            byte[] postDataBytes = x12Str.getBytes("UTF-8");
	            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
	            conn.setRequestProperty("x-api-key", HapiProperties.getProperty("x12_reader_key"));
	            conn.setRequestMethod("POST");
	            conn.setRequestProperty("Content-Type", "application/json");
	            conn.setRequestProperty("Accept", "application/json");
	            conn.setDoOutput(true);
	            conn.getOutputStream().write(postDataBytes);
	            
	            
				BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
				String line = null;
				while ((line = in.readLine()) != null) {
					sb.append(line);
				}
				System.out.println("\n x12response :"+sb.toString());
				IParser parser = FhirContext.forR4().newJsonParser();
				response = parser.parseResource(ClaimResponse.class,sb.toString());
			
		            
	            	           
	            
	     
	            
	        }
		    catch (Exception ex) {
		        ex.printStackTrace();
		    }
		return response;
	}
	

	private ClaimResponse  generateClaimResponse(Patient patient, Claim claim ) {
		ClaimResponse retVal = new ClaimResponse();
		
		
		try {
			if(patient != null){
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

			}
			
			if(claim !=null){
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
				retVal.setUse(ClaimResponse.Use.PREAUTHORIZATION );
   				retVal.setType(claim.getType());
				if(claim.getSubType() != null) {
					retVal.setSubType(claim.getSubType());
				}

			}
			
	         retVal.setCreated(new Date());
	         retVal.setUse(ClaimResponse.Use.PREAUTHORIZATION);

	         Identifier claimResIdentifier = new Identifier();
	         claimResIdentifier.setSystem("http://identifiers.mettles.com");
	         String ref = getSaltString();
	         claimResIdentifier.setValue(ref);
	         retVal.addIdentifier(claimResIdentifier);

		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
        return retVal;
	} 


	private String getPayerURL(Claim claim){
		String payerURL = "https://k8s0lhnvt2.execute-api.us-east-2.amazonaws.com/default/payer_x12";
		//from claim get "insurer reference"
		// from reference get resource
		// from resource get name
		// using name get payer from https://sm.mettles.com/cds/searchPayer
		// from the bundle recd check for endpoint resource  -- by specific
		// if end point name has "X12"
		// from this endpoint get address
		return payerURL;

	}


	private Bundle.BundleEntryComponent createTransactionEntry(Resource resource) {
        Bundle.BundleEntryComponent transactionEntry = new Bundle.BundleEntryComponent().setResource(resource);
        // if (resource.hasId()) {
        //     transactionEntry.setRequest(
        //             new Bundle.BundleEntryRequestComponent().setMethod(Bundle.HTTPVerb.PUT).setUrl(resource.getId()));
        // } else {
            transactionEntry.setRequest(new Bundle.BundleEntryRequestComponent().setMethod(Bundle.HTTPVerb.POST)
                    .setUrl(resource.fhirType()));
       // }
        return transactionEntry;
    }
	
	  // @Create
    @Operation(name = "$submit", idempotent = true)
    public Resource claimSubmit(RequestDetails details,
            @OperationParam(name = "claim", min = 1, max = 1, type = Bundle.class) Bundle bundle)
            throws RuntimeException {
    	this.errors = new JSONArray();
		Patient patient = null;
		Claim claim = null;
		Bundle responseBundle = new Bundle();
		ClaimResponse claimResponse = null;
        List<ErrorComponent> errorList= new ArrayList<ErrorComponent>();
        try {
			for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
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
			if(patient !=null){
				responseBundle.addEntry(new Bundle.BundleEntryComponent().setResource(patient));
			}
	    	ClaimResponse retVal = generateClaimResponse(patient,claim);
            if(!HapiProperties.getProperty("x12_generator_key").equals("")) {
            	IParser jsonParser = details.getFhirContext().newJsonParser();
				String jsonStr = jsonParser.encodeResourceToString(bundle);
				String x12_generated =  generateX12(jsonStr);
				if(this.errors.length() > 0) {
    	    		retVal.setStatus(ClaimResponse.ClaimResponseStatus.ENTEREDINERROR);
    	    		retVal.setOutcome(ClaimResponse.RemittanceOutcome.ERROR);
    	    		
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
					String x12SubmitUrl;
					// Need to implement the below function
					x12SubmitUrl = getPayerURL(claim);
					// Submit the generated X12 to the payer url
					String x12_response = submitX12( x12_generated, retVal,x12SubmitUrl);
					// Send the response received from X12 submission for getting relevant data
					if (x12_response.length()>0){
						// If Error on 999 received, don't store claim response
						if(x12_response.toLowerCase().contains("error")) {
							retVal.setStatus(ClaimResponse.ClaimResponseStatus.ENTEREDINERROR);
							retVal.setOutcome(ClaimResponse.RemittanceOutcome.ERROR);
							errorList.add(this.generateErrorComponent("400", x12_response));
							System.out.println(errorList.size());
							retVal.setError(errorList);
							System.out.println(retVal.getError().size());
			
						}else{

							ClaimResponse recdClaimResponse = readX12Response(x12_response);
							if (recdClaimResponse != null){
								ClaimResponse updatedResponse = updateClaimResponse(retVal,recdClaimResponse);

								// Adding the transcation to the FHIR Server
								// Bundle transactionBundle = new Bundle().setType(Bundle.BundleType.TRANSACTION);
								// transactionBundle.addEntry(createTransactionEntry(updatedResponse));
								// transactionBundle.addEntry(createTransactionEntry(patient));
								// return (Resource) this.registry.getSystemDao().transaction(details, transactionBundle);
								//return (Resource) this.bundleDao.create(transactionBundle, details).getResource();
								DaoMethodOutcome claimResponseOutcome = ClaimResponseDao.create(updatedResponse);
								claimResponse = (ClaimResponse) claimResponseOutcome.getResource();
								responseBundle.addEntry(new Bundle.BundleEntryComponent().setResource(claimResponse));

							}
							

						}

					}else{
						retVal.setStatus(ClaimResponse.ClaimResponseStatus.ENTEREDINERROR);
						retVal.setOutcome(ClaimResponse.RemittanceOutcome.ERROR);
						errorList.add(this.generateErrorComponent("400", "Internal Error"));
						System.out.println(errorList.size());
						retVal.setError(errorList);
						System.out.println(retVal.getError().size());

					}
					

    		    	
    	    	}
                
                
                
            }
            else {
            	throw new RuntimeException("API key needs to be configured for requesting X12 server");
			}
			if(claimResponse == null){
				responseBundle.addEntry(new Bundle.BundleEntryComponent().setResource(retVal));

			}
            
            return responseBundle;


        }
        catch(RuntimeException re) {
        	throw re;
        }
        catch (Exception ex) {
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
    
	private ClaimResponse updateClaimResponse(ClaimResponse currentResponse,ClaimResponse latestResponse) {
		currentResponse.setError(latestResponse.getError());
		currentResponse.setOutcome(latestResponse.getOutcome());
		currentResponse.setStatus(latestResponse.getStatus());
		if(latestResponse.getPreAuthRef() != null){
			currentResponse.setPreAuthRef(latestResponse.getPreAuthRef());
					}
	
		return currentResponse;
	}

	@Operation(name = "$build", idempotent = true)
	public  Bundle build(RequestDetails theRequestDetails,
			@OperationParam(name = "bundle", min = 1, max = 1, type =  Bundle.class) Bundle bundle) {
//		System.out.println("\n\n op call ssssss"+this.getDao());
		
		return bundle;
		
	}

	@Operation(name = "$update-claim", idempotent = true)
    public Bundle updateResponse(RequestDetails details,
            @OperationParam(name = "response", min = 1, max = 1, type = ClaimResponse.class) ClaimResponse claimResponse)
            throws RuntimeException {
				Boolean claimFound = false;
				ClaimResponse fhirClaimResponse = null;
				Bundle responseBundle = new Bundle();
				List<Identifier> claimIdentifiers = claimResponse.getIdentifier();
				for(Identifier identifier: claimIdentifiers){
					String value = identifier.getValue();
					SearchParameterMap identifierSearch = new SearchParameterMap().add("identifier",
						new TokenParam().setValue(value));
					System.out.println(" search : "+ identifierSearch.toNormalizedQueryString(FhirContext.forR4()));
					IBundleProvider bundleProvider = this.ClaimResponseDao.search(identifierSearch);
					List<IBaseResource> resources = bundleProvider.getResources(0, 10000);
					for (Iterator iterator = resources.iterator(); iterator.hasNext();) {
						fhirClaimResponse = (ClaimResponse)(iterator.next());
						System.out.println("found: "+fhirClaimResponse.toString());
						claimFound = true;
					}
            	}
				if (!claimFound){
					throw new RuntimeException("Claim Response sent doesn't match with any available ClaimResponse");
				}else{

					ClaimResponse updatedResponse = updateClaimResponse(fhirClaimResponse,claimResponse);
					ClaimResponseDao.update(updatedResponse);
					responseBundle.addEntry(new Bundle.BundleEntryComponent().setResource(updatedResponse));
					
				}
            


				return responseBundle;

			}

	
}