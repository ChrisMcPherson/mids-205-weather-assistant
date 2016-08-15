package org.apache.storm.starter.tools;

import java.io.Serializable;

import org.json.JSONObject;

/**
 * <p>
 * Provides default behaviours and implementations for the response from OWM.org
 * </p>
 */
abstract class AbstractResponse implements Serializable {
	
	/*
	 * JSON Keys
	 */
	private static final String JSON_RESPONSE_CODE = "cod";
	
	/*
	 * Instance variables
	 */
	private final int responseCode;
	private final String rawResponse;
	
	/*
	 * Constructors
	 */
	AbstractResponse() {
		this.rawResponse = null;
		this.responseCode = Integer.MIN_VALUE;
	}
	
	AbstractResponse(JSONObject jsonObj) {
		this.rawResponse = jsonObj != null ? jsonObj.toString() : null;
		this.responseCode = jsonObj != null ? jsonObj.optInt(JSON_RESPONSE_CODE, Integer.MIN_VALUE) : Integer.MIN_VALUE;
	}
	
	/**
	 * @return <code>true</code> if response is valid (downloaded and parsed correctly), otherwise <code>false</code>.
	 */
	public boolean isValid() {
		return this.responseCode == 200;
	}
	
	/**
	 * @return <code>true</code> if response code is available, otherwise <code>false</code>.
	 */
	public boolean hasResponseCode() {
		return this.responseCode != Integer.MIN_VALUE;
	}
	
	/**
	 * @return <code>true</code> if raw response is available, otherwise <code>false</code>.
	 */
	public boolean hasRawResponse() {
		return this.rawResponse != null;
	}
	
	/**
	 * @return Response code if available, otherwise <code>Integer.MIN_VALUE</code>.
	 */
	public int getResponseCode() {
		return this.responseCode;
	}
	
	/**
	 * @return Raw response if available, otherwise <code>null</code>.
	 */
	public String getRawResponse() {
		return this.rawResponse;
	}
}
