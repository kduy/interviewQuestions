package com.nventdata.task.storm;

public class ConfigurationException extends Exception {
	private static final long serialVersionUID = 1L;

	public ConfigurationException() {}
	
	public ConfigurationException(String message){
		super(message);
	}
}
