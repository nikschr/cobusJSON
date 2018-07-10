package cobusJsonProjekt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Scanner;

import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class parseTheJSON {
	
	private static String readAll(Reader rd) throws IOException {
	    StringBuilder sb = new StringBuilder();
	    int cp;
	    while ((cp = rd.read()) != -1) {
	      sb.append((char) cp);
	    }
	    return sb.toString();
	  }
	
	public static JSONObject readJsonFromUrl(String url) throws IOException, JSONException {
	    InputStream is = new URL(url).openStream();
	    try {
	      BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
	      String jsonText = readAll(rd);
	      JSONObject json = new JSONObject(jsonText);
	      return json;
	    } finally {
	      is.close();
	    }
	  }
	
	public static void printJsonObject(JSONObject jsonObj) {
	    for (Object key : jsonObj.keySet()) {
	        //based on your key types
	        String keyStr = (String)key;
	        Object keyvalue = jsonObj.get(keyStr);

	        //Print key and value
//	        if(keyStr == "company")
	        	System.out.println("key: "+ keyStr + " value: " + keyvalue);
	        
	        //for nested objects iteration if required
	        if (keyvalue instanceof JSONObject)
	            printJsonObject((JSONObject)keyvalue);
	    }
	}

	public static void main(String[] args) throws IOException, SQLException {
		
		URL url = new URL("http://www.json-generator.com/api/json/get/bUbmMVOGyG?indent=2");
		
		String sqlConnectionString;
		String compName;
		
		//JDBC Objekte
		Connection connection = null;
		ResultSet resultSet = null;
		Statement selectStmt = null;
		CallableStatement stmt = null;
		
		//config Datei einlesen
		File configFile = new File("C:\\Users\\CUH-GWX9\\git\\cobusJSON\\cobusJsonProjekt\\src\\cobusJsonProjekt\\config.properties");
		FileReader reader = new FileReader(configFile);
		Properties props = new Properties();
		props.load(reader);
		
		//connection String für die Verbindung zum SQL Server aufbauen
		sqlConnectionString = props.getProperty("general") + props.getProperty("database") + props.getProperty("user") + props.getProperty("password");
		
		//Verbindung zum SQL server herstellen
		connection = DriverManager.getConnection(sqlConnectionString);
		System.out.println("Connected");
		
		//reading JSON-content via URL Connection & Inputstream
		URLConnection con = url.openConnection();
		InputStream in = con.getInputStream();
		String encoding = con.getContentEncoding();
		encoding = encoding == null ? "UTF-8" : encoding;
		String body = IOUtils.toString(in, encoding);	
		
		//reading JSON-content via scanner
		Scanner scan = new Scanner(url.openStream());
		String str = new String();
		while(scan.hasNext())
			str += scan.nextLine();
		scan.close();
		
		//JSONObject beginnt ab '{' statt bei '['
		JSONObject jsonObj = new JSONObject(body.substring(body.indexOf('{')));
		
		//Select Statement aus der Config lesen
		String sqlSelect = props.getProperty("selectString");
        selectStmt = connection.createStatement();
        resultSet = selectStmt.executeQuery(sqlSelect);
        
		try {
			JSONArray jsonArray = new JSONArray(body);
 
			int count = jsonArray.length(); // get totalCount of all jsonObjects
			for(int i=0 ; i< count; i++){   // iterate through jsonArray 
				int isDublicate = 0;
				JSONObject jsonObject = jsonArray.getJSONObject(i);  // get jsonObject @ i position 
//				System.out.println("jsonObject " + i + ": " + jsonObject);
				String company = jsonObject.getString("company");
				String email = jsonObject.getString("email");
				
				while(resultSet.next()) {
		        	compName = resultSet.getString("COMPNAME");
		        	if(compName == company) {
		        		isDublicate = 1;
		        		break;
		        	}else {
		        		isDublicate = 0;
		        	}
		        }
				
				if(isDublicate == 0) {
					String callProc = "{call dbo.cobus (?, ?)}";
					stmt = connection.prepareCall(callProc);
					stmt.setString(1, company);
					stmt.setString(2,  email);
	                stmt.execute();
				}else {
					System.out.println("Die Firma" + company + "gibt es bereits im System");
				}
				
				System.out.println(company);
				String address = jsonObject.getString("address");
				System.out.println(address);
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
	}

}
