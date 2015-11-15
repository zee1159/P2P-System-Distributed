/***************************************************************************************\
 * Name			 : Zeeshan Aamir Khavas
 * Application	 : Distributed Hash Table P2P Application
 * Program		 : Manager.java
 * Description   : This class is a runnable thread. It is instantiated for each client/server
 * 				   connection requests. It handles all the client/server requests by calling
 * 				   their respective methods.
 * Date			 : 11/03/2015
 * @author Zee
\***************************************************************************************/

package com.zee.pdht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Hashtable;
import java.util.Map.Entry;


public class Manager implements Runnable {
	private int set;	//Flag to register peer and create server connections
	private Socket myClient;
	private String key;
	LocalHash hash;
	DataInputStream clientIn;
	DataOutputStream clientOut;

	/*Hash tables to store the Socket IO streams of the other connected servers*/
	private Hashtable<Integer, DataOutputStream> servers = new Hashtable<Integer, DataOutputStream>();
	private Hashtable<Integer, DataInputStream> serversIn = new Hashtable<Integer, DataInputStream>();

	/*Default constructor*/
	public Manager(){

	}

	/* **********************************************************************
	 * Method Name 	:	Manager
	 * Parameters	:	Socket, String, LocalHash
	 * Returns		:	void
	 * Description	:	Parameterized constructor that will set the
	 * 					client/server values
	 * **********************************************************************/
	public Manager(Socket myClient, String key, LocalHash hash) {
		this.myClient = myClient;
		this.key = key;
		this.hash = hash;
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
		try {
			set = 0;
			processClient();	//Calling method to distinguish requests from client/server
		}
		catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			System.out.println("Some Error Occured ! Restart the disconnected peer: \nPeer [ " + (myClient.getInetAddress()).getHostAddress() + ":" + myClient.getPort() + " ] disconnected !");
			//e.printStackTrace();
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			System.out.println("Some Error Occured ! Restart the disconnected peer: \nPeer [ " + (myClient.getInetAddress()).getHostAddress() + ":" + myClient.getPort() + " ] disconnected !");
			//e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			System.out.println("Some Error Occured ! Restart the disconnected peer: \nPeer [ " + (myClient.getInetAddress()).getHostAddress() + ":" + myClient.getPort() + " ] disconnected !");
			//e.printStackTrace();
		}
	}

	/* **********************************************************************
	 * Method Name 	:	processCLient
	 * Parameters	:	No parameters
	 * Returns		:	void
	 * Description	:	Method to accept requests from client/server and distinguish
	 * 					the client/server requests. Then call respective methods
	 * 				 	for processing requests.
	 * **********************************************************************/
	private void processClient() throws NoSuchAlgorithmException, NumberFormatException, InterruptedException {
		String message;

		try {
			//Instantiating the socket IO stream objects for the thread
			clientIn = new DataInputStream(myClient.getInputStream());
			clientOut = new DataOutputStream(myClient.getOutputStream());

			//till the client connection with server is bound, it will listen for requests from client
			while(myClient.isBound()){
				message = clientIn.readUTF();

				//Calling request handler based on messages from SERVER or CLIENT
				if(message.equalsIgnoreCase("CLIENT")){
					clientHandler(clientIn, clientOut);		//Calling client method
				}
				else if(message.equalsIgnoreCase("SERVER")){
					serverHandler(clientIn, clientOut);		//Calling server method
				}

			}

		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Peer [ " + (myClient.getInetAddress()).getHostAddress() + ":" + myClient.getPort() + " ] disconnected !");
			//e.printStackTrace();
		}

	}

	/* **********************************************************************
	 * Method Name 	:	clientHandler
	 * Parameters	:	DataInputStream, DataOutputStream
	 * Returns		:	void
	 * Description	:	Method to process following requests from client:
	 * 					1. Registering the client
	 * 					2. Update the files from client
	 * 					3. Provide search results
	 * 					4. Delete file from the distributed table
	 * 					5. Removing client on closing connection
	 * **********************************************************************/
	private void clientHandler(DataInputStream clientIn, DataOutputStream clientOut) throws IOException, NoSuchAlgorithmException, NumberFormatException, InterruptedException {

		String pair;
		pair = ((myClient.getInetAddress()).getHostAddress()) + ":" + (myClient.getPort());	//Details of connected peer

		/*Check if all the servers are connected,
		 *if not call method to connect all servers.
		 *Add peer to the peer list table */
		if(set == 0){
			connectServers(key);	//Method to connect all other servers
			hash.addPeer(pair);		//Adding peer to the hashtable - register
		}



		String message = clientIn.readUTF();

		//"PUT" request will add the key-value pairs received from client to hash table
		if(message.equalsIgnoreCase("PUT")){
			int size, val, repVal, hashCode;

			size = clientIn.readInt();		//Reading the number of files, the client will update
			String hashString, result, fileName;
			boolean res;

			for(int i = 0; i < size; i++){
				fileName = clientIn.readUTF();

				hashCode = fileName.hashCode();		//generating hashcode of the file name
				val = hashCode % 8;			//mod value of hashcaode determines where the file will be stored
				if(val < 0)
					val = -val;

				hashString = calculateSha(fileName);	//Generating SHA-1 hash value of file name

				//Sending the filename and peer details to respective server
				if(val == Integer.parseInt(key) || (Integer.parseInt(key) - val) == 8){
					res = hash.put(hashString, pair);	//Adds the key value pair to local hash table of server
					if(res == true)
						clientOut.writeUTF("TRUE");
					else
						clientOut.writeUTF("FALSE");
					repVal = val +1;
					//Sending another copy of the file to be stored on other server for consistency
					result = connectToServer(repVal, hashString, pair, message, mapper(repVal), mapperIn(repVal) );
				}
				else{
					if(val == 7)
						repVal = 0;
					else
						repVal = val + 1;
					//Sends the key-value pair to the other servers for storing it in their local hash table
					result = connectToServer(val, hashString, pair, message, mapper(val), mapperIn(val) );
					clientOut.writeUTF(result);

					//Sending another copy of the file to be stored on other server for consistency
					if(repVal == Integer.parseInt(key) || (Integer.parseInt(key) - repVal) == 8){
						res = hash.put(hashString, pair);
					}
					else{
						result = connectToServer(repVal, hashString, pair, message, mapper(repVal), mapperIn(repVal) );
					}
				}

			}
		}
		//"GET" request will retrieve the key-value pairs for the key received from client
		else if(message.equalsIgnoreCase("GET")){
			int val, hashCode;
			String res, hashString, fileName, host, port;
			String temp, newList = "";
			String[] peerList, tempPeer;

			host = (myClient.getInetAddress()).getHostAddress();
			port = String.valueOf(myClient.getPort());

			fileName = clientIn.readUTF();

			hashCode = fileName.hashCode();		//generating hashcode of the file name
			val = hashCode % 8;					//mod value of hashcaode determines where the file will be stored
			if(val < 0)
				val = -val;
			hashString = calculateSha(fileName);	//Generating SHA-1 hash value of file name

			if(val == Integer.parseInt(key) || (Integer.parseInt(key) - val) == 8){
				temp = hash.get(hashString);	//Retrieves key-value pair from the local hash table
				if(!temp.isEmpty()){
					peerList = temp.split(",");
					for(String current : peerList){
						tempPeer = current.split(":");
						if(!(tempPeer[0].equals(host) && tempPeer[1].equals(port)))
							newList += current + ",";
					}
				}
				clientOut.writeUTF(newList);		//Sends the list of peers having requested file
			}
			else{
				//Sends the key-value pair to the other servers for storing it in their local hash table
				res = connectToServer(val, hashString, null, message, mapper(val), mapperIn(val));

				//Removes current peer details from the list
				if(!res.isEmpty()){
					peerList = res.split(",");
					for(String current : peerList){
						tempPeer = current.split(":");
						if(!(tempPeer[0].equals(host) && tempPeer[1].equals(port))){
							newList += current + ",";
						}
					}
				}
				clientOut.writeUTF(newList);	//Sends the list of peers having requested file
			}
		}
		//"DEL" request will remove the key-value pairs for the key received from client
		else if(message.equalsIgnoreCase("DEL")){
			String fileName;

			String hashString, result;
			int val, hashCode, repVal;
			boolean res;

			fileName = clientIn.readUTF();
			hashCode = fileName.hashCode();	//Generates hashcode of the file name
			val = hashCode % 8;				//Calculates the mod value for the hashcode of file name received
			if(val < 0)
				val = -val;
			hashString = calculateSha(fileName);		//Calculates the hash value of the key received

			if(val == Integer.parseInt(key) || (Integer.parseInt(key) - val) == 8){
				res = hash.del(hashString, pair);	//Deletes key-value pair from the local hash table
				if(res == true)
					clientOut.writeUTF("TRUE");
				else
					clientOut.writeUTF("FALSE");
				repVal = val + 1;
				//Deletes key-value pair from the replicate file name value in other server hash table
				result = connectToServer(repVal, hashString, pair, message, mapper(repVal), mapperIn(repVal) );
			}
			else{
				if(val == 7)
					repVal = 0;
				else
					repVal = val + 1;
				//Sends the key to the other servers for removing key-value pair from their local hash table
				result = connectToServer(val, hashString, pair, message, mapper(val), mapperIn(val));
				clientOut.writeUTF(result);

				//Deletes key-value pair from the replicate file name value in other server hash table
				if(repVal == Integer.parseInt(key) || (Integer.parseInt(key) - repVal) == 8){
					res = hash.del(hashString, pair);
				}
				else{
					result = connectToServer(repVal, hashString, pair, message, mapper(repVal), mapperIn(repVal) );
				}
			}
		}
		//"REPLICATE" request will provide a list of peers where the client can create replicates of the file
		else if(message.equalsIgnoreCase("REPLICATE")){
			String temp, res, cHost, cPort;
			String[] peerList, peerAdd;
			String newList = "";
			int num, count = 0;

			num = clientIn.readInt();

			cHost = (myClient.getInetAddress()).getHostAddress();
			cPort = String.valueOf(myClient.getPort());

			temp = hash.replicate();

			//Gets the list of clients that are connected to other servers
			for(int i = 1; i <= 8; i++){
				if(i != Integer.parseInt(key)){
					res = connectToServer(i, null, null, message, mapper(i), mapperIn(i));
					if(res != null)
						temp += res;
				}
			}

			peerList = temp.split(",");

			//Removes the client that has requested replicate from the list of peers
			if((peerList.length - 1) >= num){
				for(String peers : peerList){
					peerAdd = peers.split(":");
					if(!(peerAdd[0].equals(cHost) && peerAdd[1].equals(cPort))){
						newList += peers + ",";
						count++;
					}
					if(count == num)
						break;
				}
				clientOut.writeUTF(newList);	//Sending list to the client
			}
			else{
				clientOut.writeUTF("");	//Sending list to the client
			}
		}
		//"CLOSE" request will disconnects the client from server
		else if(message.equalsIgnoreCase("CLOSE")){
			myClient.close();
			hash.removePeer(pair);
			informServer(message, pair);	//Requesting other servers to remove the client details from their hashtables
			//System.out.println("Peer [ " + (myClient.getInetAddress()).getHostAddress() + ":" + myClient.getPort() + " ] disconnected !");
		}
	}

	/* **********************************************************************
	 * Method Name 	:	calculateSha
	 * Parameters	:	String
	 * Returns		:	String
	 * Description	:	Method to hash value for the parameter passed using
	 * 					SHA-1 algorithm
	 * **********************************************************************/
	private String calculateSha(String locKey) throws NoSuchAlgorithmException {
		MessageDigest mDigest = MessageDigest.getInstance("SHA1");
        byte[] result = mDigest.digest(locKey.getBytes());
        StringBuffer shRes = new StringBuffer();
        for (int i = 0; i < result.length; i++) {
        	shRes.append(Integer.toString((result[i] & 0xff) + 0x100, 16).substring(1));
        }
        return (shRes.toString());
	}

	/* **********************************************************************
	 * Method Name 	:	informServer
	 * Parameters	:	String, String
	 * Returns		:	void
	 * Description	:	Method to sending all other servers request that the
	 * 					upcoming requests.
	 * **********************************************************************/
	private void informServer(String message, String pair) throws IOException {
		DataOutputStream servOut;
		int key;
		for(Entry<Integer, DataOutputStream> entry: servers.entrySet()){
			key = entry.getKey();
			servOut = entry.getValue();
			servOut.writeUTF("SERVER");
			servOut.writeUTF(message);
			servOut.writeUTF(pair);
		}
	}

	/* **********************************************************************
	 * Method Name 	:	serverHandler
	 * Parameters	:	DataInputStream, DataOutputStream
	 * Returns		:	void
	 * Description	:	Method to sending all other servers request that the
	 * 					upcoming requests are either PUT or GET or DEL
	 * **********************************************************************/
	private void serverHandler(DataInputStream clientIn, DataOutputStream clientOut) throws IOException {
		String message = clientIn.readUTF();
		String[] keyVal;
		String hashString, res;
		boolean response;

		//"PUT" request will add the file name and peer details received from server to hash table
		if(message.equalsIgnoreCase("PUT")){
			keyVal = clientIn.readUTF().split(":");
			response = hash.put(keyVal[0], (keyVal[1]+":"+keyVal[2]));	//Adds the key value pair to local hash table of server
			if(response == true)
				clientOut.writeUTF("TRUE");
			else
				clientOut.writeUTF("FALSE");
		}

		//"GET" request will retrieve the peer details for the file name received from client
		else if(message.equalsIgnoreCase("GET")){
			hashString = clientIn.readUTF();
			res = hash.get(hashString);		//Retrieves peer details from the local hash table
			clientOut.writeUTF(res);
		}

		//"DEL" request will remove the file name and peer details for the file name received from client
		else if(message.equalsIgnoreCase("DEL")){
			keyVal = clientIn.readUTF().split(":");
			response = hash.del(keyVal[0], (keyVal[1]+":"+keyVal[2]));	//Deletes file name and peer details from the local hash table
			if(response == true)
				clientOut.writeUTF("TRUE");
			else
				clientOut.writeUTF("FALSE");
		}

		//"CLOSE" request will disconnects the other servers connected to this server
		else if(message.equalsIgnoreCase("CLOSE")){
			hashString = clientIn.readUTF();
			hash.removePeer(hashString);
		}
		//"REPLICATE" request will fetch the available clients at the server
		else if(message.equalsIgnoreCase("REPLICATE")){
			String temp;
			temp = hash.replicate();
			clientOut.writeUTF(temp);
		}

	}

	/* **********************************************************************
	 * Method Name 	:	connectToServer
	 * Parameters	:	int, String, String, String, DataInputStream, DataOutputStream
	 * Returns		:	String
	 * Description	:	Method to sending all other servers key-value pairs for
	 * 					operations PUT or GET or DEL
	 * **********************************************************************/
	private String connectToServer(int val, String hashString, String keyVal, String message, DataOutputStream servOut, DataInputStream servIn) throws NumberFormatException, UnknownHostException, IOException, NoSuchAlgorithmException, InterruptedException {
		String res = null;

		if (val == 0){
			val = 8;
		}

		//Sending file name and peer details pairs for PUT operation
		if(message.equalsIgnoreCase("PUT")){
			servOut.writeUTF("SERVER");
			servOut.writeUTF("PUT");
			servOut.writeUTF(hashString +":"+ keyVal);
			res = servIn.readUTF();
		}
		//Sending file name and peer details for GET operation
		else if(message.equalsIgnoreCase("GET")){
			try{
			servOut.writeUTF("SERVER");
			servOut.writeUTF("GET");
			servOut.writeUTF(hashString);
			res = servIn.readUTF();
			}
			catch(IOException e){
				res = backupConnect(val, hashString, message);	//Calling the operation on backup server
			}
		}
		//Sending file name and peer details for DEL operation
		else if(message.equalsIgnoreCase("DEL")){
			servOut.writeUTF("SERVER");
			servOut.writeUTF("DEL");
			servOut.writeUTF(hashString +":"+ keyVal);
			res = servIn.readUTF();
		}
		//Sending file name and peer details for REPLICATE operation
		else if(message.equalsIgnoreCase("REPLICATE")){
			servOut.writeUTF("SERVER");
			servOut.writeUTF("REPLICATE");
			res = servIn.readUTF();

		}

		return res;		//Returning the response received from the server
	}


	/* **********************************************************************
	 * Method Name 	:	backupConnect
	 * Parameters	:	int, String, String
	 * Returns		:	String
	 * Description	:	Method to send the requested operation to the
	 * 					backup server if the original one fails.
	 * **********************************************************************/
	private String backupConnect(int val, String hashString, String message) throws NumberFormatException, UnknownHostException, NoSuchAlgorithmException, IOException, InterruptedException {
		// TODO Auto-generated method stub
		String res = null;

		if(val == 7)
			val = 0;
		else
			val = val + 1;

		if(val == Integer.parseInt(key) || (Integer.parseInt(key) - val) == 8){
			res = hash.get(hashString);
		}
		else{
			res = connectToServer(val, hashString, null, message, mapper(val), mapperIn(val));
		}

		return res;
	}

	/* **********************************************************************
	 * Method Name 	:	connectServers
	 * Parameters	:	String
	 * Returns		:	void
	 * Description	:	Method to creating connection with rest of the servers
	 * 					in the server cluster.
	 * **********************************************************************/
	private void connectServers(String key) throws NumberFormatException, UnknownHostException, IOException {

		ConfigReader prop = new ConfigReader();
		String[] name, temp;
		String[] server = new String[7];
		int counter = 0;
		Socket servTemp;
		DataOutputStream servOut;
		DataInputStream servIn;

		//Getting the address and port of all the servers from the config file
		for (int i = 1; i <= 8; i++){
			if(i != Integer.parseInt(key)){
				name = prop.read(String.valueOf(i));
				server[counter] = i + ":" + name[0] + ":" + name[1];
				counter++;
			}
		}

		//Creating socket connections with rest of server in the cluster
		for(int i = 0; i < 7; i++){
			temp = server[i].split(":");
			servTemp = new Socket(temp[1], Integer.parseInt(temp[2]));
			servOut = new DataOutputStream(servTemp.getOutputStream());
			servIn = new DataInputStream(servTemp.getInputStream());
			servers.put(Integer.parseInt(temp[0]), servOut);	//Adding Output streams to the hash table
			serversIn.put(Integer.parseInt(temp[0]), servIn);	//Adding input streams to the hash table
		}

		set = 1;	//Setting the flag indicating that servers are connected
	}

	/* **********************************************************************
	 * Method Name 	:	mapper
	 * Parameters	:	int
	 * Returns		:	DataOutputStream
	 * Description	:	Method to retrieve output streams from the hash table.
	 * **********************************************************************/
	public DataOutputStream mapper(int val){
		if(val == 0){
			val = 8;
		}
		return servers.get(val);
	}

	/* **********************************************************************
	 * Method Name 	:	mapperIn
	 * Parameters	:	int
	 * Returns		:	DataInputStream
	 * Description	:	Method to retrieve input streams from the hash table.
	 * **********************************************************************/
	public DataInputStream mapperIn(int val){
		if(val == 0){
			val = 8;
		}
		return serversIn.get(val);
	}


}
