/****************************************************************************************
 * Name			 : Zeeshan Aamir Khavas
 * Application	 : Distributed Hash Table P2P Application
 * Program		 : Client.java
 * Description   : This is the main class for the Client application.
 * 				   It connects to user requested server and also starts local port to
 * 				   listen other peer requests. Accepts connections from other peers and
 * 				   creates thread for each peer.
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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

public class Client {
	/*Global variables*/
	static int key = 1;
	static ClientManager locManager;

	public static void main(String[] args) throws ClassNotFoundException {
		// TODO Auto-generated method stub

		int id, myId;
		String serverName;
		ServerSocket locServer;
		Socket myClient;
		InputStream serverIn;
		DataInputStream clientIn;
		DataOutputStream clientOut;

		Scanner scan = new Scanner(System.in);

		/*User is prompted for server address and details*/
		System.out.println("+-----------------------------------------+");
		System.out.println("|** Welcome to Decentralized P2P Client **|");
		System.out.println("+-----------------------------------------+");
		System.out.println("Enter Server address:");
		serverName = scan.nextLine();
		System.out.println("Enter Server port:");
		id = Integer.parseInt(scan.nextLine());

		try {

			myClient = new Socket(serverName, id);		//connection is made to the requested server
			myId = myClient.getLocalPort();				//local connected port is retrieved
			myId++;										//new port is created for other peer connections
			locServer = new ServerSocket(myId);			//local server is started to listen other peer requests
			locManager = new ClientManager(locServer);
			Thread t = new Thread(locManager);			//a thread is created for the local server
			t.start();

			/*Server IO streams are instantiated*/
			serverIn = myClient.getInputStream();
			clientIn = new DataInputStream(serverIn);
			clientOut = new DataOutputStream(myClient.getOutputStream());

			//if server connection is successful display message
			System.out.println("\n---@ Connection to Server successful ! @---");
			System.out.println("\nPlease wait updating files to be shared on server...");

			long lStartTime = System.currentTimeMillis();
			serverUpdate(clientOut, clientIn);		//serverUpdate() will send file names to the server
			long lEndTime = System.currentTimeMillis();
			long difference = lEndTime - lStartTime;
			System.out.println("Elapsed time for file details updating on server: " + (difference) + " ms");

			while(key != 4){
				displayMenu(myClient, clientOut, clientIn);	//display() will show user a application menu to user
			}

			clientOut.writeUTF("CLOSE");		//if client wishes to close connection send server close request
			System.out.println("\n*   *   *   *   *   *   *   *   *   *   *   *   *   *   *");
			System.out.println("Thank you for using P2P client. Your connection is closed !");
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Connection to Server failed ! ");
			//e.printStackTrace();
		}
		scan.close();

	}

	/* **********************************************************************
	 * Method Name 	:	displayMenu
	 * Parameters	:	Socket, DataOutputStream, DataInputStream
	 * Returns		:	void
	 * Description	:	This method will display a application menu to the user.
	 * 					It takes input from user and calls respective method to
	 * 					handle requests.
	 * **********************************************************************/
	public static void displayMenu(Socket myClient, DataOutputStream clientOut, DataInputStream clientIn) throws ClassNotFoundException, IOException {
		// TODO Auto-generated method stub

		Scanner scan = new Scanner(System.in);

		/*Display menu*/
		System.out.println("\n+-------------------------+");
		System.out.println("|        P2P Menu         |");
		System.out.println("+-------------------------+");
		System.out.println("|    1. Search file       |");
		System.out.println("|    2. Delete file       |");
		System.out.println("|    3. Replicate file    |");
		System.out.println("|    4. Exit network      |");
		System.out.println("+-------------------------+");
		System.out.println("Enter the selection number:");
		key = scan.nextInt();
		clientOut.writeUTF("CLIENT");

		/*based on user selection call respective methods*/
		switch(key){
		case 1:
			searchFile(myClient, clientOut, clientIn);	//Method to search files on network
			break;
		case 2:
			deleteFile(myClient, clientOut, clientIn);	//Method to delete files on server
			break;
		case 3:
			replicateFile(myClient, clientOut, clientIn);	//Method to replicating the files
			break;
		}
	}

	/* **********************************************************************
	 * Method Name 	:	displayMenu
	 * Parameters	:	Socket, DataOutputStream, DataInputStream
	 * Returns		:	void
	 * Description	:	This method will delete the user requested file from
	 * 					the server table.
	 * **********************************************************************/
	private static void deleteFile(Socket myClient, DataOutputStream clientOut, DataInputStream clientIn) throws IOException {
		// TODO Auto-generated method stub
		String fileName, res;
		Scanner scan = new Scanner(System.in);

		System.out.println("Enter the file name to be deleted: ");
		fileName = scan.nextLine();

		/*Check for empty file names*/
		while(fileName.isEmpty()){
			System.out.println("Enter a valid file name !");
			fileName = scan.nextLine();
		}

		clientOut.writeUTF("DEL");
		clientOut.writeUTF(fileName);	//sending file name to the server
		res = clientIn.readUTF();		//Receiving delete ACK from server

		if(res.equalsIgnoreCase("TRUE"))
			System.out.println("File - " + fileName +  " deleted successfully !");
		else
			System.out.println("File delete unsuccessful, some error occured !");
	}

	/* **********************************************************************
	 * Method Name 	:	replicateFile
	 * Parameters	:	Socket, DataOutputStream, DataInputStream
	 * Returns		:	void
	 * Description	:	This method will send replicate request to server and
	 * 					retrieves peer list. Then it will send requested file
	 * 					to peer list.
	 * **********************************************************************/
	private static void replicateFile(Socket myClient, DataOutputStream clientOut, DataInputStream clientIn) throws IOException {
		// TODO Auto-generated method stub
		String fileName;
		int num;
		String host, res;
		int port;
		String[] currentPeers, peerList;

		Scanner scan = new Scanner(System.in);

		/*Take file name and number of copies to be replicated from user*/
		System.out.println("Enter the file name with extension to replicate: ");
		fileName = scan.nextLine();
		System.out.println("Enter number of copies to be replicated: ");
		num = scan.nextInt();

		/*Check for empty file names*/
		while(fileName.isEmpty()){
			System.out.println("Enter a valid file name !");
			fileName = scan.nextLine();
		}

			clientOut.writeUTF("REPLICATE");			//send server replicate request
			clientOut.writeInt(num);					//send replication factor
			res = clientIn.readUTF();

			/*If peers are available for replication, send files to peers*/
			if(!res.isEmpty()){
				peerList = res.split(",");

				for(String peer : peerList){
					currentPeers = peer.split(":");

					Socket repClient;
					DataOutputStream repOut;
					DataInputStream repIn;
					FileInputStream fis;
					BufferedInputStream bis;
					OutputStream ois;

					host = currentPeers[0];
					port = Integer.parseInt(currentPeers[1]);
					port++;							//get port number of the local peer server

					repClient = new Socket(host, port);		//connect to local peer server
					repOut = new DataOutputStream(repClient.getOutputStream());
					repIn = new DataInputStream(repClient.getInputStream());
					repOut.writeUTF("REPLICATE");			//send replicate request to the peer
					repOut.writeUTF(fileName);				//send name of file to be replicated
					ois = repClient.getOutputStream();
					String folder = "up", file;
					file = folder +"/" + fileName;

					/*Search for file directory if doesn't exist create*/
					File directory = new File("up");
					if(!directory.exists()){
						directory.mkdir();
					}

					/*Send the file to peer using buffer stream*/
					File myFile = new File(file);

			        byte [] mybytearray  = new byte [(int)myFile.length()];
			        repOut.writeInt((int)myFile.length());;

			        fis = new FileInputStream(myFile);
			        bis = new BufferedInputStream(fis);
			        bis.read(mybytearray,0,mybytearray.length);
			        ois.write(mybytearray,0,mybytearray.length);
			        ois.flush();

			        bis.close();
			        fis.close();
			        ois.close();
			        repClient.close();
				}
				System.out.println("File replicated successfully !");
			}
			else{
				System.out.println("File cannot be replicated !");
				System.out.println("Peers are not available or less than the replicates requested !");
			}
	}

	/* **********************************************************************
	 * Method Name 	:	searchFile
	 * Parameters	:	Socket, DataOutputStream, DataInputStream
	 * Returns		:	void
	 * Description	:	This method will search the requested file in the
	 * 					network sending request to server.
	 * **********************************************************************/
	private static void searchFile(Socket myClient, DataOutputStream clientOut, DataInputStream clientIn) throws IOException, ClassNotFoundException {
		// TODO Auto-generated method stub
		String fileName;

		Scanner scan = new Scanner(System.in);

		/*Take name of file to be searched from the user*/
		System.out.println("Enter the file name with extension to search: ");
		fileName = scan.nextLine();

		if(fileName.isEmpty()){
			System.out.println("Enter a valid file name !");
		}
		else{
			long lStartTime = System.currentTimeMillis();
			clientOut.writeUTF("GET");		//send search request to server
			clientOut.writeUTF(fileName);		//send name of file to be searched
			searchResults(myClient, fileName, clientOut, clientIn, lStartTime);		//searchResults() will display search results
		}
	}

	/* **********************************************************************
	 * Method Name 	:	searchResults
	 * Parameters	:	Socket, String, DataOutputStream, DataInputStream, long
	 * Returns		:	void
	 * Description	:	This method will retrieve the search results from
	 * 					server and displays it to user for further processing.
	 * **********************************************************************/
	private static void searchResults(Socket myClient, String fileName, DataOutputStream clientOut, DataInputStream clientIn, long lStartTime) throws IOException, ClassNotFoundException {
		// TODO Auto-generated method stub

		int counter = 0, sel;
		int pPort;
		String pHost, peers;

		String[] currentPeers = new String[2], temp;			//variable to save peer details
		String tempObj;
		Hashtable<Integer, String[]> peerList = new Hashtable<Integer, String[]>();	//a local hash table to save the retrieved peer details
		Scanner scan = new Scanner(System.in);

		peers = clientIn.readUTF();		//Receiving peer list form the server

		long lEndTime = System.currentTimeMillis();
		long difference = lEndTime - lStartTime;
		System.out.println("Elapsed time for file Search: " + (difference) + " ms");
		boolean rep = peers.isEmpty();

		//Displaying the list of peers having the file
		if(!rep){
			temp = peers.split(",");
			int size = temp.length;
			System.out.println("\nFile - '" + fileName + "' is available at the following peers: ");
			System.out.println("+------------------------------------+");
			System.out.println("| ID       Host Name         Port    |");
			System.out.println("+------------------------------------+");

			for(int i = 0; i < size; i++){
				counter++;
				tempObj = temp[i];			//get peer details
				currentPeers = tempObj.split(":");
				peerList.put(counter, currentPeers);	//add peer to the local hash table
				System.out.println("|  " + counter + "       " + currentPeers[0] + "          " + currentPeers[1] + "  |");
			}
			System.out.println("+------------------------------------+");

			System.out.println("Enter the ID of the peer to download file from it:");	//prompt user for peer from where file have to be downloaded
			sel = scan.nextInt();

			currentPeers = peerList.get(sel);	//get the peer details from corresponding selection
			pHost = currentPeers[0];
			pPort = Integer.parseInt(currentPeers[1]);
			pPort++;
			download(pHost, pPort, fileName);	//download() will download file form the selected peer
		}
		/*If file not available at any peer display message*/
		else{
			System.out.println("+------------------------------------+");
			System.out.println("# File not avilable at any peers ! #");
			System.out.println("+------------------------------------+");
			displayMenu(myClient, clientOut, clientIn);
		}
	}

	/* **********************************************************************
	 * Method Name 	:	download
	 * Parameters	:	String, int, String
	 * Returns		:	void
	 * Description	:	This method will download file from the requested
	 * 					peer address and port.
	 * **********************************************************************/
	private static void download(String pHost, int pPort, String fileName) throws UnknownHostException, IOException {
		// TODO Auto-generated method stub
		Socket dnClient;
		DataOutputStream dnOut;
		DataInputStream dnIn;
		InputStream dnInput;
		BufferedOutputStream buffOut = null;
		FileOutputStream fileOut = null;

		File directory = new File("down");
		String down = "down/" + fileName;

		/*Search for download directory if doesn't exist create*/
		if(!directory.exists()){
			directory.mkdir();
		}

		dnClient = new Socket(pHost, pPort);		//connect to the peer

		/*Server IO streams are instantiated*/
		dnOut = new DataOutputStream(dnClient.getOutputStream());
		dnIn = new DataInputStream(dnClient.getInputStream());
		long lStartTime = System.currentTimeMillis();
		dnOut.writeUTF("GET");			//send download request
		dnOut.writeUTF(fileName);		//send name of file to be downloaded
		int fileSize = dnIn.readInt();	//read size of file

		int bytesRead;
	    int current = 0;

	    /*Initiate file receive using buffered stream*/
	    try {
	      System.out.println("\nRecieving file " + fileName + "...!");
	      byte [] mybytearray  = new byte [fileSize];
	      dnInput = dnClient.getInputStream();
	      fileOut = new FileOutputStream(down);
	      buffOut = new BufferedOutputStream(fileOut);
	      bytesRead = dnInput.read(mybytearray,0,mybytearray.length);
	      current = bytesRead;

	      do {
	         bytesRead = dnInput.read(mybytearray, current, (mybytearray.length-current));
	         if(bytesRead >= 0){
	        	 current += bytesRead;
	         }
	      } while(bytesRead > 0);

	      buffOut.write(mybytearray, 0 , current);		//write the downloaded file
	      buffOut.flush();
	      long lEndTime = System.currentTimeMillis();
	      long difference = lEndTime - lStartTime;
	      System.out.println("Elapsed time for file download: " + (difference) + " ms");
	      System.out.println("File - " + fileName + " downloaded successfully !");
	    }
	    finally {
	    	dnClient.close();
	        if (buffOut != null) buffOut.close();
	      }
	}

	/* **********************************************************************
	 * Method Name 	:	serverUpdate
	 * Parameters	:	DataOutputStream, DataInputStream
	 * Returns		:	void
	 * Description	:	This method will update the server
	 * 					with files available the client for sharing
	 * **********************************************************************/
	private static void serverUpdate(DataOutputStream clientOut, DataInputStream clientIn) throws IOException {
		// TODO Auto-generated method stub
		String result;
		int chk = 0;
		clientOut.writeUTF("CLIENT");
		clientOut.writeUTF("PUT");		//send file update request to server

		File directory = new File("up");

		/*Search for upload directory if doesn't exist create*/
		if(!directory.exists()){
			directory.mkdir();
		}

		File[] files = directory.listFiles();		//List all the files in upload directory

		clientOut.writeInt(files.length);

		for(File file : files)
		{
		    String name = file.getName();
		    clientOut.writeUTF(name);				//send file names to the server
		    result = clientIn.readUTF();
			if(result.equalsIgnoreCase("FALSE")){
				System.out.println("Update Operation failed for file: " + name);
				chk = 1;
			}
		}
		if(chk == 1)
			System.out.println("File update operation was unsuccesssful !");
		else
			System.out.println("File update operation was successful !");
	}

}
