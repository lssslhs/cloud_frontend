package org.jboss.as.quickstarts.Myapp;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Decoder {
	
	private String _secretKey ;
	private ConcurrentHashMap<String,String> keyTable ; 
	private String miniKey ;
	private String encryptKey;
	
	public Decoder(String secretKey) {
		this._secretKey = secretKey;
		this.keyTable = new ConcurrentHashMap<String, String>(1000000, 0.7f, 8);
	}
	
	private int getRoot(int number) {
		return (int)Math.sqrt(number);
	}
	
	private String reorder(String normalString) {
		int rootNum = getRoot(normalString.length());
		StringBuilder spiralString = new StringBuilder("");

		//first pattern
		spiralString.append(normalString.substring(0,rootNum));
		//if count == 0 change step
		int count = 2;

		//if step == 0 end
		int step = rootNum - 1;

		//if tempStep == 0 change count and direction
		int tempStep = step ;

		/*
			0: right, 
			1: down, 
			2: left,
			3: up
		*/
		int direction = 1; //initial direction

		//initial position in the string
		int stringIndex = rootNum - 1;

		//index action according to direction
		int[] indexAction = new int[]{1,rootNum,-1,-rootNum};

		while(step!=0) {
			//find next position
			stringIndex += indexAction[direction];
			spiralString.append(normalString.charAt(stringIndex));
			tempStep--;

			//change count and direction
			if(tempStep==0) {
				count--;
				direction = (direction+1) % 4;
				//change step
				if(count==0) {
					step--;
					count = 2;
				}
				tempStep = step;
			}
		} 
		return spiralString.toString();
	}
	
	private void getEncryptKeyAndMiniKey(String key) {
		if(!this.keyTable.containsKey(key)) {
			this.encryptKey = new BigInteger(key).divide(new BigInteger(this._secretKey)).toString();
			this.miniKey = new BigInteger(this.encryptKey).mod(new BigInteger("25")).add(BigInteger.ONE).toString();
			this.keyTable.put(key, this.miniKey);
		}
		else {
			this.miniKey = (String)this.keyTable.get(key);
		}	
	}
	
	private String decode(String message) {
		StringBuilder newMsg = new StringBuilder("");
		for(int i=0; i<message.length(); i++) {
			int charCode = (int)message.charAt(i);
			if((charCode- Integer.parseInt(this.miniKey))<65) {
				int remain = Integer.parseInt(this.miniKey) - (charCode-65);
				charCode = 91 - remain;
			}
			else {
				charCode -=Integer.parseInt(this.miniKey);
			}
			newMsg.append((char)charCode);
		}
		return newMsg.toString();
	}
	public String getOriginalString(String key, String message) {
		getEncryptKeyAndMiniKey(key);
		String originalString = decode(reorder(message));
		return originalString;
	}
}
