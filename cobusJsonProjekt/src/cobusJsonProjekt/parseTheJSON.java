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
import javax.swing.*;

public class parseTheJSON {
//	Diese Funktion übergibt einen Firmennamen aus den JSON-Daten und übergibt diesen an den SQL Server
//	auf dem SQL Server wird die SP dbo.ExistsComp aufgerufen und der JSON-Firmenname wird mit den bestehenden Datensätzen abgeglichen
	public static boolean dublettenCheck(JSONObject jsonObj, Connection con) throws SQLException {
		String query = "{call dbo.ExistsComp (?, ?)}";
		String jsonCompany = jsonObj.getString("company");
		String jsonCompanyLower = jsonCompany.toLowerCase();
		
		CallableStatement compareComp = con.prepareCall(query);
		compareComp.setString(1, jsonCompanyLower);
		compareComp.registerOutParameter(2,  java.sql.Types.BOOLEAN);
		compareComp.execute();	
		return compareComp.getBoolean(2);
	}
	
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
		Statement selectStmtAddress0 = null;
		CallableStatement stmtInsertCompany = null;
		CallableStatement stmtInsertTableGuid = null;
		CallableStatement stmtInsertChangeLog = null;
		
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
		
		//JSON per URL auslesen
		URLConnection con = url.openConnection();
		InputStream in = con.getInputStream();
		String encoding = con.getContentEncoding();
		encoding = encoding == null ? "UTF-8" : encoding;
		String jsonContent = IOUtils.toString(in, encoding);	
		
		//Select Statement aus der Config lesen
		String sqlSelect = props.getProperty("selectString");
        selectStmtAddress0 = connection.createStatement();
        
        try {
			JSONArray jsonArray = new JSONArray(jsonContent);
			
			int count = jsonArray.length(); // get totalCount of all jsonObjects
			for(int i=0 ; i < count; i++){   // iterate through jsonArray 
				resultSetAddress0 = selectStmtAddress0.executeQuery(sqlSelect);
				JSONObject jsonObject = jsonArray.getJSONObject(i);  // get jsonObject @ i position 
				String jsonCompany = jsonObject.getString("company");
				String jsonEmail = jsonObject.getString("email");
				boolean dublette = dublettenCheck(jsonObject, connection);
				
				System.out.println(dublette);
				
				if(dublette == true) {
	                System.out.println("Die Firma " + jsonCompany + " gibt es bereits im System");
				}else {
					String callSpCobus = "{call dbo.cobus (?, ?)}";
					stmtInsertCompany = connection.prepareCall(callSpCobus);
					stmtInsertCompany.setString(1, jsonCompany);
					stmtInsertCompany.setString(2,  jsonEmail);
	                stmtInsertCompany.execute();
	                
	                String callSpChangeLog = "{call dbo.cobusChangeLog (?)}";
	                stmtInsertChangeLog = connection.prepareCall(callSpChangeLog);
	                stmtInsertChangeLog.setString(1, jsonCompany);
	                stmtInsertChangeLog.execute();
	                
					while(resultSetAddress0.next()) {
						String tableGuid = resultSetAddress0.getString("GGUID");
						String callSpOrel = "{call dbo.cobusOrel (?)}";
						stmtInsertTableGuid = connection.prepareCall(callSpOrel);
						stmtInsertTableGuid.setString(1, tableGuid);
		                stmtInsertTableGuid.execute();
					}
				}       
			}          
		}catch (JSONException e) {
			e.printStackTrace();
		}finally {
            if (resultSetAddress0 != null) try { resultSetAddress0.close(); } catch(Exception e) {}  
            if (selectStmtAddress0 != null) try { selectStmtAddress0.close(); } catch(Exception e) {}
            if (stmtInsertCompany != null) try { stmtInsertCompany.close(); } catch(Exception e) {}
            if (stmtInsertTableGuid != null) try { stmtInsertTableGuid.close(); } catch(Exception e) {}
            if (stmtInsertChangeLog != null) try { stmtInsertChangeLog.close(); } catch(Exception e) {}
            if (connection != null) try { connection.close(); } catch(Exception e){}
            System.out.println("Connection closed");
        }
	}
}
