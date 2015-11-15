/****************************************************************************************
 * Name			 : Zeeshan Aamir Khavas
 * Application	 : Distributed Hash Table P2P Application
 * Program		 : ClientParser.java
 * Description   : This class is a runnable thread. It will process requests from other peers.
 * Date			 : 11/03/2015
 * @author Zee
 ***************************************************************************************/

package com.zee.pdht;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public class ClientParser implements Runnable {

	Socket myClient;
	DataInputStream clientIn;
	DataOutputStream clientOut;

	/* **********************************************************************
	 * Method Name 	:	ClientParser
	 * Parameters	:	Socket
	 * Description	:	Parameterized constructor that will set the client socket
	 * **********************************************************************/
	public ClientParser(Socket myClient){
		this.myClient = myClient;
	}

	/* **********************************************************************
	 * Method Name 	:	parser
	 * Parameters	:	No parameters
	 * Returns		:	void
	 * Description	:	This method will take client requests and process it.
	 * **********************************************************************/
	private void parser() throws IOException{
		// TODO Auto-generated method stub

		/*Server IO streams are instantiated*/
		try {
			clientIn = new DataInputStream(myClient.getInputStream());
			clientOut = new DataOutputStream(myClient.getOutputStream());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String fileName;
		FileInputStream fis;
		FileOutputStream fileOut = null;
		BufferedInputStream bis;
		BufferedOutputStream buffOut = null;
		OutputStream ois;
		String folder = "up", file;

		/*Till the peer is available at port accept requests*/
		while(clientIn.available() != 0){

			String code;
			code = clientIn.readUTF();			//read the request from peer
			ois = myClient.getOutputStream();

			/*"GET" request will send the requested file to the peer*/
			if(code.equalsIgnoreCase("GET")){
				fileName = clientIn.readUTF();

				file = folder + "/" + fileName;
				File myFile = new File(file);

				/*Initiate file send using buffered stream*/
		        byte [] mybytearray  = new byte [(int)myFile.length()];
		        clientOut.writeInt((int)myFile.length());
		        fis = new FileInputStream(myFile);
		        bis = new BufferedInputStream(fis);
		        bis.read(mybytearray,0,mybytearray.length);
		        ois.write(mybytearray,0,mybytearray.length);
		        ois.flush();
			}

			/*"REPLICATE" request will receive the file that has to be replicated from peer */
			else if(code.equalsIgnoreCase("REPLICATE")){
				fileName = clientIn.readUTF();

				String down = "up/" + fileName;
				int fileSize = clientIn.readInt();
				int bytesRead;
			    int current = 0;

			    /*Initiate file send using buffered stream*/
			    try {
			      byte [] mybytearray  = new byte [fileSize];
			      fileOut = new FileOutputStream(down);
			      buffOut = new BufferedOutputStream(fileOut);
			      bytesRead = clientIn.read(mybytearray,0,mybytearray.length);
			      current = bytesRead;

			      do {
			         bytesRead = clientIn.read(mybytearray, current, (mybytearray.length-current));
			         if(bytesRead >= 0){
			        	 current += bytesRead;
			         }
			      } while(bytesRead > 0); //changed from 0

			      buffOut.write(mybytearray, 0 , current);
			      buffOut.flush();
			    }
			    finally {
			        if (buffOut != null) buffOut.close();
			        if (fileOut != null) fileOut.close();
			      }
			}
		}

	}

	/* **********************************************************************
	 * Method Name 	:	run
	 * Parameters	:	No parameters
	 * Returns		:	void
	 * Description	:	This method is will be executed first in a thread
	 * 					execution.
	 * **********************************************************************/
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			parser();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
