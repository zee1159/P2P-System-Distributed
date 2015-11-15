/***************************************************************************************\
 * Name			 : Zeeshan Aamir Khavas
 * Application	 : Distributed Hash Table P2P Application
 * Program		 : LocalHash.java
 * Description   : This class creates a concurrent hash map and performs the operations
 * 				   on the concurrent hash map.
 * Date			 : 11/03/2015
 * @author Zee
\***************************************************************************************/

package com.zee.pdht;

import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;


public class LocalHash {

	/*Creating a concurrent hash map for storing the key-value pairs*/
	private ConcurrentHashMap<String, String> peerList;
	private ConcurrentHashMap<String, ArrayList<String>> fileList;

	/*Default Constructor to instantiate the hash map*/
	public LocalHash(){
		this.setFileList(new ConcurrentHashMap<String, ArrayList<String>>());
		this.setPeerList(new ConcurrentHashMap<String, String>());
	}

	/*Accessors and modifiers*/
	public ConcurrentHashMap<String, ArrayList<String>> getFileList() {
		return fileList;
	}

	public void setFileList(ConcurrentHashMap<String, ArrayList<String>> fileList) {
		this.fileList = fileList;
	}

	public ConcurrentHashMap<String, String> getPeerList() {
		return peerList;
	}

	public void setPeerList(ConcurrentHashMap<String, String> peerList) {
		this.peerList = peerList;
	}

	/* **********************************************************************
	 * Method Name 	:	addPeers
	 * Parameters	:	String
	 * Returns		:	int
	 * Description	:	This method will add the peers to the concurrent hashmap
	 * 					local to the server.
	 * **********************************************************************/
	public int addPeer(String peer) {
		String host, port;
		int id = 0;
		String[] temp;

		temp = peer.split(":");

		/*Iterate through the peerList table to check if the peer already exists*/
		for(Entry<String, String> entry: peerList.entrySet()){
			port = (entry.getValue());
			host = entry.getKey();
			if((temp[0].equals(host)) && (temp[1].equals(port))){
				id = 1;
				break;
			}
		}

		if(id > 0)
			return id;		//If peer found, return 1 indicating peer already exists
		else{
			host = temp[0];		//add peer info to the peer object
			port = temp[1];
			getPeerList().put(host, port);	//add the peer to local concurrent hashmap
			return 2;
		}
	}

	/* **********************************************************************
	 * Method Name 	:	removePeer
	 * Parameters	:	String
	 * Returns		:	void
	 * Description	:	This method will remove peer details of
	 * 					corresponding peer details from the local concurrent hashmap.
	 * **********************************************************************/
	public void removePeer(String peer) {
		// TODO Auto-generated method stub
		String file;
		String[] temp, peerDel;

		ArrayList<String> currentList = new ArrayList<String>();

		temp = peer.split(":");
		peerList.remove(temp[0]);		//Removes peer from local concurrent hashmap

		Set<String> keys = getFileList().keySet();
		Iterator<String> itr = keys.iterator();

		/*Iterate through fileList table and removes the peer*/
		while (itr.hasNext()) {
			file = itr.next();
			currentList = getFileList().get(file);
			for(int i = 0; i < currentList.size(); i++){
				Iterator<String> it = currentList.iterator();
	            while (it.hasNext()) {
	            	String val = it.next();
	            	peerDel = val.split(":");
	                if (peerDel[0].equals(temp[0]) && peerDel[1].equals(temp[1])) {
	                    it.remove();
	                }
	            }
			}
		}
	}


	/* **********************************************************************
	 * Method Name 	:	put
	 * Parameters	:	String, String
	 * Returns		:	Boolean
	 * Description	:	Method to add a file name and peer details in
	 * 					concurrent hash map.
	 * **********************************************************************/
	public Boolean put(String fileName, String pair) {
		String file;
		int chk = 0;

		ArrayList<String> currentList = new ArrayList<String>();

		/*Iterate through the fileList table to check if the file already exists*/
		for(Entry<String, ArrayList<String>> entry: fileList.entrySet()){
			file = entry.getKey();
			if(file.equalsIgnoreCase(fileName)){
				chk = 1;
				break;
			}
		}

		if(chk == 1){		//if file exists in table add the peer to the existing entry.
			currentList = fileList.get(fileName);
			currentList.add(pair);
			fileList.put(fileName, currentList);
			return true;
		}
		else{				//if file doesn't exists in table, create new entry in the table.
			currentList.add(pair);
			fileList.put(fileName, currentList);
			return true;
		}
	}

	/* **********************************************************************
	 * Method Name 	:	get
	 * Parameters	:	String
	 * Returns		:	String
	 * Description	:	Method to retrieve a peer details from concurrent
	 * 					hash map for the requested file name.
	 * **********************************************************************/
	public String get(String key) {
		String peers = "";
		int size;
		ArrayList<String> temp;

		temp = getFileList().get(key);
		if(temp != null){
			size = temp.size();

			for(String peer : temp){
				peers += peer + ",";
			}
		}
		return peers;
	}

	/* **********************************************************************
	 * Method Name 	:	del
	 * Parameters	:	String
	 * Returns		:	Boolean
	 * Description	:	Method to remove peer details from concurrent hash map
	 * **********************************************************************/
	public Boolean del(String key, String pair) {
		String file;
		String[] peer, temp;
		int chk = 0;
		ArrayList<String> currentList = new ArrayList<String>();

		temp = pair.split(":");

		Set<String> keys = fileList.keySet();
		Iterator<String> itr = keys.iterator();

		/*Iterate through fileList table and removes the peer id*/

		while (itr.hasNext()) {
			file = itr.next();
			if(file.equals(key)){
				currentList = fileList.get(file);
				Iterator<String> it = currentList.iterator();
		        while (it.hasNext()) {
		        	peer = it.next().split(":");
		            if (peer[0].equals(temp[0]) && peer[1].equals(temp[1])) {
		            	it.remove();
		                chk = 1;
		                break;
		            }
		        }
		        break;
			}
		}
		if(currentList.isEmpty()){
			getFileList().remove(key);
		}
		else{
			getFileList().put(key, currentList);
		}

		if(chk == 1)
			return true;
		else
			return false;
	}

	/* **********************************************************************
	 * Method Name 	:	replicate
	 * Parameters	:	none
	 * Returns		:	String
	 * Description	:	Method to send list of peer details from concurrent
	 * 					hash map
	 * **********************************************************************/
	public String replicate() {
		// TODO Auto-generated method stub
		String peers = "";
		for(Entry<String, String> entry : peerList.entrySet()){
			peers += (entry.getKey() + ":" + entry.getValue() + ",");
		}
		return peers;
	}

}
