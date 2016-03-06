/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013, Red Hat, Inc. and/or its affiliates, and individual
 * contributors by the @authors tag. See the copyright.txt in the
 * distribution for a full listing of individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jboss.as.quickstarts.Myapp;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.KeyValue;



@SuppressWarnings("serial")
@WebServlet("/")
public class Myapp extends HttpServlet {
    
	@Inject
    HelloService hello;
    Decoder decoder;
	
	//cluster ip address
	private static String computer1 = "http://ec2-52-6-69-85.compute-1.amazonaws.com";
	private static String computer2 = "http://ec2-52-5-138-218.compute-1.amazonaws.com";
	private static String computer3 = "http://ec2-52-6-14-55.compute-1.amazonaws.com"; 
	private static String computer4 = "http://ec2-52-6-69-101.compute-1.amazonaws.com";
	private static String computer5 = "http://ec2-52-6-59-68.compute-1.amazonaws.com";
	
	private static String q3Computer = "http://ec2-52-5-46-137.compute-1.amazonaws.com";
	private static String q4Computer = "http://ec2-52-6-61-191.compute-1.amazonaws.com";
	private static String q5Computer = "http://ec2-52-4-51-53.compute-1.amazonaws.com";
	private static String q6Computer = "http://ec2-52-6-62-139.compute-1.amazonaws.com";
	
	
    private static boolean _useMysql = true;
    
    //tables
    private static ConcurrentHashMap<String,String> q3Table ;
    private static ConcurrentHashMap<String,String> q4Table ;
    private static ConcurrentHashMap<String,String> q5Table ;
    private static ConcurrentHashMap<String,String> q6Table ;
    
    //private static ConcurrentHashMap<String,String> testTable ;
    
    //sql parameters
    private String sqlPort = "3306";
	private String sqlUser = "user";
	private String sqlpw = "password";
	private String dbName = "db_test";
	
	//sql address
	private static HikariDataSource _hds3 ;
    private String sqlurl3 = "ec2-52-4-248-163.compute-1.amazonaws.com";
	private static HikariDataSource _hds4 ;
    private String sqlurl4 = "ec2-52-4-127-147.compute-1.amazonaws.com";
    private static HikariDataSource _hds5 ;
    private String sqlurl5 = "ec2-52-5-35-75.compute-1.amazonaws.com";
    private static HikariDataSource _hds6 ;
    private String sqlurl6 = "ec2-52-5-188-246.compute-1.amazonaws.com";
	
	//sql query
	private static String _sqlQuery3 = "select txt from q3_data where id = ?";
	private static String _sqlQuery4 = "select txt from q4_data where tag = ?";
	private static String _sqlQuery5 = "select txt from q5_data where id = ?";
	private static String _sqlQuery6_1 = "select txt from q6_data where id >= ? limit 1";
	private static String _sqlQuery6_2 = "select txt from q6_data where id <= ? order by id desc limit 1";
	
	//hbase parameters
	private String hbaseMasterNode = "ec2-52-4-223-250.compute-1.amazonaws.com";
	private HTablePool hpool ;
	
	private static final String _firstLine = "etc,6572-3240-1412,7308-6447-5680\n";
	
	String[] seperateDates = new String[]{"2014-05-03","2014-05-16","2014-05-29","2014-06-11"};
    long[] urlTimeStamp ;
    
    @Override
    public void init() throws ServletException {
        System.out.println("server start");
        decoder = new Decoder("8271997208960872478735181815578166723519929177896558845922250595511921395049126920528021164569045773");
        q3Table = new ConcurrentHashMap<String, String>(5000000, 0.7f, 8);
        q4Table = new ConcurrentHashMap<String, String>(1000000, 0.7f, 8);
        q5Table = new ConcurrentHashMap<String, String>(1000000, 0.7f, 8);
        q6Table = new ConcurrentHashMap<String, String>(1000000, 0.7f, 8);
        
        urlTimeStamp = new long[seperateDates.length];
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UCT"));
        for(int i=0; i<seperateDates.length; i++) {
        	try {
				urlTimeStamp[i] = dateFormat.parse(seperateDates[i]).getTime();
				System.out.println("url Time stamp "+i+" "+urlTimeStamp[i]);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        
        if(_useMysql) {
    		//config
        }
        else {
        	Configuration config = HBaseConfiguration.create();
        	config.set("hbase.zookeeper.quorum",hbaseMasterNode);
        	hpool = new HTablePool(config,100);
        }
        
      }
    
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	
    	StringBuilder replyString = new StringBuilder();
    	replyString.append(_firstLine);
    	String requestUrl = req.getRequestURI();
    	
    	if(requestUrl.contains("favicon.ico")){
            return;
    	}
    	
    	if(requestUrl.contains("q1")) {
    		if(req.getQueryString()!=null) {
    			String key = req.getParameter("key");
    	    	String message = req.getParameter("message");
    	    	if(key!=null&&message!=null) {
    	    		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    	        	Calendar calendar = new GregorianCalendar();
    	        	calendar.setTimeZone(TimeZone.getTimeZone("EST"));
    	        	calendar.add(Calendar.HOUR,3);
    	    		replyString.append(dateFormat.format(calendar.getTime()));
    	    		replyString.append("\n");
    	    		replyString.append(decoder.getOriginalString(key, message));
    	    		replyString.append("\n");
    	    	}
    		}
    	}
    	else if(requestUrl.contains("q2")) {
    		if(req.getQueryString()!=null) {
    			String uid = req.getParameter("userid");
    			String tweetTime = req.getParameter("tweet_time");
    			if(uid!=null && tweetTime!=null) {
    				forwardRequest(resp,requestUrl,uid,tweetTime);
    		        return;
    			}
    		}
    	}
    	else if(requestUrl.contains("q3")) {
    		if(req.getQueryString()!=null) {
    			String uid = req.getParameter("userid");
    			String tempUrl = q3Computer+requestUrl+"?"+"userid="+uid;
    			resp.sendRedirect(tempUrl);
    			
                return;
    		}
    	}
    	else if(requestUrl.contains("q4")) {
			if(req.getQueryString()!=null) {
				String tag = req.getParameter("hashtag");
				String startTime = req.getParameter("start");
				String endTime = req.getParameter("end");
    			String tempUrl = q4Computer+requestUrl+"?"+"hashtag="+tag+"&start="+startTime+"&end="+endTime;
    			resp.sendRedirect(tempUrl);
                return ;
			}
    	}
    	else if(requestUrl.contains("q5")) {
    		if(req.getQueryString()!=null) {
    			String uidlist = req.getParameter("userlist");
    			String startDate = req.getParameter("start");
				String endDate = req.getParameter("end");
				String tempUrl = q5Computer + requestUrl + "?" + "userlist="+uidlist+"&start="+startDate+"&end="+endDate;
				resp.sendRedirect(tempUrl);
				
                return;
    		}
    	}
    	else if(requestUrl.contains("q6")) {
    		if(req.getQueryString()!=null) {
    			String m = req.getParameter("m");
    			String n = req.getParameter("n");
    			String tempUrl = q6Computer+requestUrl+"?"+"m="+m+"&n="+n;
    			resp.sendRedirect(tempUrl);

    			return;
    		}
    	}

		resp.setContentType("text/plaintext");
		resp.setCharacterEncoding("UTF-8");
		ServletOutputStream out = resp.getOutputStream();
		PrintWriter writer = new PrintWriter(new OutputStreamWriter(out,
				"UTF-8"));
		writer.print(replyString.toString());
		writer.flush();
		writer.close();
    }   

    public void forwardRequest(HttpServletResponse resp, String requestUrl, String uid, String time) throws IOException{
    	String tempTime = time.substring(0,10);
    	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		dateFormat.setTimeZone(TimeZone.getTimeZone("UCT"));
		long timeStamp = 0;
		try {
			timeStamp = dateFormat.parse(tempTime).getTime();			
		} catch (ParseException e) {
			e.printStackTrace();
		}
		String tempUrl = "";
		time = time.substring(0,10)+"+"+time.substring(11);
		if(timeStamp<urlTimeStamp[0]) {
			tempUrl = computer1+requestUrl+"?"+"userid="+uid+"&tweet_time="+time;
		}
		else if(timeStamp>=urlTimeStamp[0]&&timeStamp<urlTimeStamp[1]){
			tempUrl = computer2+requestUrl+"?"+"userid="+uid+"&tweet_time="+time;
		}
		else if(timeStamp>=urlTimeStamp[1]&&timeStamp<urlTimeStamp[2]){
			tempUrl = computer3+requestUrl+"?"+"userid="+uid+"&tweet_time="+time;
		}
		else if(timeStamp>=urlTimeStamp[2]&&timeStamp<urlTimeStamp[3]){
			tempUrl = computer4+requestUrl+"?"+"userid="+uid+"&tweet_time="+time;
		}
		else if(timeStamp>=urlTimeStamp[3]){
			tempUrl = computer5+requestUrl+"?"+"userid="+uid+"&tweet_time="+time;
		}
		resp.sendRedirect(tempUrl);
    }
    
    public static class UserScoreInfo implements Comparable<UserScoreInfo>{
    	public String uid ;
    	public int score;
    	
    	public UserScoreInfo(String uid, int score) {
    		this.uid = uid;
    		this.score = score;
    	}
    	
    	
    	@Override
		public int compareTo(UserScoreInfo other) {
	        if(this.score>other.score) {
	        	return -1;
	        }
	        else {
	        	return 1;
	        }
	    }
    }
}
