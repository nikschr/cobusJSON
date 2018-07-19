package cobusJsonProjekt;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class parseTheJSON {
	
	public static boolean compareCompanies(JSONObject jsonObject, ResultSet rs) throws SQLException{
		boolean companiesMatch = false;
		String jsonCompany = jsonObject.getString("company");
		String jsonCompanyLower = jsonCompany.toLowerCase();
		
		while(rs.next()) {
			String sqlCompany = rs.getString("COMPNAME");
			String sqlCompanyLower = sqlCompany.toLowerCase();
			if(sqlCompanyLower.equals(jsonCompanyLower)) {
				companiesMatch = true;
				break;
			}else {
				companiesMatch = false;
			}
		}
		return companiesMatch;
	}
	
	public static boolean compareGuids(String guidAddress0, ResultSet rs) throws SQLException {
		boolean guidsMatch = true;
		
		while(rs.next()) {
			String guidAddressOrel = rs.getString("TABLEGUID");
			if(guidAddressOrel.equals(guidAddress0)) {
				guidsMatch = true;
				break;
			}else {
				guidsMatch = false;
			}
		}
		return guidsMatch;
	}

	public static void main(String[] args) throws IOException, SQLException {
		
		URL url = new URL("http://www.json-generator.com/api/json/get/bUbmMVOGyG?indent=2");
		
		String sqlConnectionString;

		//JDBC Objekte
		Connection connection = null;
		ResultSet resultSetAddress0 = null;
		ResultSet resultSetAddressOrel = null;
		Statement selectStmtAddress0 = null;
		Statement selectStmtAddressOrel = null;
		CallableStatement stmtInsertCompany = null;
		CallableStatement stmtInsertTableGuid = null;
		
		//config Datei einlesen
		File configFile = new File("C:\\Users\\CUH-GWX9\\git\\cobusJSON\\cobusJsonProjekt\\src\\cobusJsonProjekt\\config.properties");
		FileReader reader = new FileReader(configFile);
		Properties props = new Properties();
		props.load(reader);
		
		//connection String f�r die Verbindung zum SQL Server aufbauen
		sqlConnectionString = props.getProperty("general") + props.getProperty("database") + props.getProperty("user") + props.getProperty("password");
		
		//Verbindung zum SQL server herstellen
		connection = DriverManager.getConnection(sqlConnectionString);
		System.out.println("Connected");
		
		//reading JSON-content via URL Connection & Inputstream
		URLConnection con = url.openConnection();
		InputStream in = con.getInputStream();
		String encoding = con.getContentEncoding();
		encoding = encoding == null ? "UTF-8" : encoding;
		String jsonContent = IOUtils.toString(in, encoding);	
		
		//Select Statement aus der Config lesen
		String sqlSelect = props.getProperty("selectString");
        selectStmtAddress0 = connection.createStatement();
        
        //Select Statement aus der Config f�r die AddressOrel Tabelle auslesen
        String sqlSelectOrel = props.getProperty("selectStringOrel");	
        
        try {
			JSONArray jsonArray = new JSONArray(jsonContent);
			
			int count = jsonArray.length(); // get totalCount of all jsonObjects
			for(int i=0 ; i < count; i++){   // iterate through jsonArray 
				resultSetAddress0 = selectStmtAddress0.executeQuery(sqlSelect);
				JSONObject jsonObject = jsonArray.getJSONObject(i);  // get jsonObject @ i position 
				String jsonCompany = jsonObject.getString("company");
				String jsonEmail = jsonObject.getString("email");
				boolean companiesMatch = compareCompanies(jsonObject, resultSetAddress0);
				
		        String tableGuidSelect = "Select GGUID from ADDRESS0 where compname = '" + jsonCompany +"'"; 
				
				if(companiesMatch == true) {
	                System.out.println("Die Firma " + jsonCompany + " gibt es bereits im System");
				}else {
					String callSpCobus = "{call dbo.cobus (?, ?)}";
					stmtInsertCompany = connection.prepareCall(callSpCobus);
					stmtInsertCompany.setString(1, jsonCompany);
					stmtInsertCompany.setString(2,  jsonEmail);
	                stmtInsertCompany.execute();			        
				}
		        selectStmtAddressOrel = connection.createStatement();
				resultSetAddressOrel = selectStmtAddressOrel.executeQuery(sqlSelectOrel);
				
				if(!resultSetAddressOrel.next()) {
					System.out.println("Das ResultSet ist leer");
				} else {
			        String tableGuid = resultSetAddress0.getString("GGUID");
			        boolean guidsMatch = compareGuids(tableGuid, resultSetAddressOrel);
			        System.out.println(guidsMatch);
			        tableGuid = "0x" + tableGuid;
			        if(guidsMatch == true) {
			        	System.out.println("Die GUID " + tableGuid + " gibt es bereits in AddressOrel");
			        } else {
//			        	String callProcOrel = "{call dbo.cobusOrel (?)}";
//						stmtInsertTableGuid = connection.prepareCall(callProcOrel);
//						stmtInsertTableGuid.setString(1, tableGuid);
//		                stmtInsertTableGuid.execute();
			        }				
				}	        
			}          
		}catch (JSONException e) {
			e.printStackTrace();
		}finally {
            if (resultSetAddress0 != null) try { resultSetAddress0.close(); } catch(Exception e) {}  
            if (resultSetAddressOrel != null) try { resultSetAddressOrel.close(); } catch(Exception e) {}  
            if (selectStmtAddress0 != null) try { selectStmtAddress0.close(); } catch(Exception e) {}
            if (selectStmtAddressOrel != null) try { selectStmtAddressOrel.close(); } catch(Exception e) {}
            if (stmtInsertCompany != null) try { stmtInsertCompany.close(); } catch(Exception e) {}
            if (stmtInsertTableGuid != null) try { stmtInsertTableGuid.close(); } catch(Exception e) {}
            if (connection != null) try { connection.close(); } catch(Exception e){}
            System.out.println("Connection closed");
        }
	}
}
