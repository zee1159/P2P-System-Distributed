/***************************************************************************************\
 * Name			 : Zeeshan Aamir Khavas
 * Application	 : Distributed Hash Table P2P Application
 * Program		 : ConfigReader.java
 * Description   : This class reads the config file.
 * Date			 : 11/03/2015
 * @author Zee
\***************************************************************************************/

package com.zee.pdht;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigReader {
	//Default constructor
	public ConfigReader(){

	}

	/* **********************************************************************
	 * Method Name 	:	read
	 * Parameters	:	String
	 * Returns		:	String[]
	 * Description	:	Method to parse the config file for the requested server
	 * 					identifier and add the retrieved server details to a String[].
	 * **********************************************************************/
	public String[] read(String key){
		Properties prop = new Properties();
		InputStream input = null;
		String id = "";
		String[] server = new String[2];
		try {
			input = new FileInputStream("config.properties");	//Creating object of the config file
			prop.load(input);
			id = prop.getProperty(key);
			server = id.split(",");
		}
		catch (IOException ex) {
			ex.printStackTrace();
		}

		return server;
	}
}
