/****************************************************************************************\
 * Name			 : Zeeshan Aamir Khavas
 * Application	 : Distributed Hash Table P2P Application (Decentralized)
 * Program		 : Server.java
 * Description   : This is the main class for the DHT Server application.
 * 				   It starts the Server by taking user defined identifier.Accepts
 * 				   connections from clients/servers and creates thread for each accepted
 * 				   connection.
 * Date			 : 11/03/2015
 * * @author Zee
 \***************************************************************************************/

package com.zee.pdht;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;


public class Server {

	/* main function of the server */
	public static void main(String[] args) {
		ServerSocket myServer;
		Socket myClient;
		Manager myManager;
		int port;

		String[] id = new String[2];

		//Instantiating the Config file reader class object
		ConfigReader prop = new ConfigReader();

		Scanner scan = new Scanner(System.in);

		System.out.println("+-----------------------------------------+");
		System.out.println("*** Welcome to Decentralized P2P Server ***");
		System.out.println("+-----------------------------------------+");
		System.out.println("Enter server id (1 - 8) to be started: ");	//Takes port as user input
		String key = scan.nextLine();
		id = prop.read(key); 	//Reads the config file for port of the selected server
		port = Integer.parseInt(id[1]);

		System.out.println("Starting Server Number " + key + "...");

		try {
			InetAddress serverAdd = InetAddress.getLocalHost();	//InetAddrress will return the IP of the server
			myServer = new ServerSocket(port);
			System.out.println("\n::Server is active on the following address & port::");
			System.out.println("Server Address : " + serverAdd.getHostAddress());
			System.out.println("Server Port    : " + myServer.getLocalPort());

			/*A Hash Table local to the server is created
			 * for storing the key value pair relative to
			 * this server.
			 */
			LocalHash hash = new LocalHash();

			System.out.println("\n---@ Node Activity @---");

			/* A loop will keep Server connection open.
			 * It accepts clients connections to the server
			 */
			while(true){
				myClient = myServer.accept();	//Accept connections from clients and other server
				System.out.println("Peer [ " + (myClient.getInetAddress()).getHostAddress() + ":" + myClient.getPort() + " ] connected...");
				myManager = new Manager(myClient, key, hash);			//A manager object will handle all the client/server requests
				Thread t = new Thread(myManager);					//A new thread is created for each client/server connection
				t.start();
			}
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		scan.close();
	}

}
