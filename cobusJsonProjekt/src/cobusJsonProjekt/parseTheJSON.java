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

import com.microsoft.sqlserver.jdbc.SQLServerDataTable;

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
	
	public static void main(String[] args) throws IOException, SQLException {
		
		URL url = new URL("http://www.json-generator.com/api/json/get/clQVAeNGzS?indent=2");	
		String sqlConnectionString;

		//JDBC Objekte
		Connection connection = null;
		Statement selectStmtAddress0 = null;
		CallableStatement stmtInsertCompany = null;
		CallableStatement stmtInsertTableGuid = null;
		CallableStatement stmtInsertChangeLog = null;
		
		//config Datei einlesen
		File configFile = new File("C:\\Users\\CUH-GWX9\\git\\cobusJSON\\cobusJsonProjekt\\src\\cobusJsonProjekt\\config.properties");
		FileReader reader = new FileReader(configFile);
		Properties props = new Properties();
		props.load(reader);
		
		//connection String für die Verbindung zum SQL Server bilden
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
        
		SQLServerDataTable cobusDataList = new SQLServerDataTable();
		
//		cobusDataList.addColumnMetadata("companyName", java.sql.Types.NVARCHAR);
//		cobusDataList.addColumnMetadata("companyZip", java.sql.Types.NVARCHAR);
//		cobusDataList.addColumnMetadata("companyCity", java.sql.Types.NVARCHAR);
//		cobusDataList.addColumnMetadata("companyCountryCode", java.sql.Types.NVARCHAR);
//		cobusDataList.addColumnMetadata("companyBranch", java.sql.Types.NVARCHAR);
//		cobusDataList.addColumnMetadata("companyContacts", java.sql.Types.NVARCHAR);
//		cobusDataList.addColumnMetadata("companyPhone", java.sql.Types.NVARCHAR);
//		cobusDataList.addColumnMetadata("companyAddress", java.sql.Types.NVARCHAR);
//		cobusDataList.addColumnMetadata("companyId", java.sql.Types.BIGINT);
//		cobusDataList.addColumnMetadata("companyDomain", java.sql.Types.NVARCHAR);
//		cobusDataList.addColumnMetadata("companySize", java.sql.Types.NVARCHAR);
//		cobusDataList.addColumnMetadata("companyFoundingYear", java.sql.Types.SMALLINT);
//		cobusDataList.addColumnMetadata("companyRevenue", java.sql.Types.NVARCHAR);
//		cobusDataList.addColumnMetadata("tags", java.sql.Types.NVARCHAR);
//		cobusDataList.addColumnMetadata("noteValue", java.sql.Types.NVARCHAR);
//		cobusDataList.addColumnMetadata("rating", java.sql.Types.SMALLINT);
//		cobusDataList.addColumnMetadata("scoreMaxPercentage", java.sql.Types.TINYINT);
//		cobusDataList.addColumnMetadata("scoreAvg", java.sql.Types.TINYINT);
//		cobusDataList.addColumnMetadata("visits", java.sql.Types.INTEGER);
//		cobusDataList.addColumnMetadata("visitIdList", java.sql.Types.NVARCHAR);
//		
//		JSONArray jsonArray = new JSONArray(jsonContent);
//		
//		int count = jsonArray.length(); // Anzahl der JsonObjekte
//		for(int i=0 ; i < count; i++){   // Durch den JsonArray iterieren
//			JSONObject jsonObject = jsonArray.getJSONObject(i);  // get jsonObject @ i position 
//			String jsonCompany = jsonObject.getString("companyName");
//			String jsonZip = jsonObject.getString("companyZip");
//			String jsonCitWy = jsonObject.getString("companyCity");
//			String jsonCountryCode = jsonObject.getString("companyCountryCode");
//			String jsonBranch = jsonObject.getString("companyBranch");
//			String jsonContacts = jsonObject.getString("companyContacts");
//			String jsonPhone = jsonObject.getString("companyPhone");
//			String jsonAddress = jsonObject.getString("companyAddress");
//			long jsonCompanyId = jsonObject.getBigInteger("companyId");
//			String jsonDomain = jsonObject.getString("companyDomain");
//			String jsonCompanySize = jsonObject.getString("companySize");
//			int jsonFoundingYear = jsonObject.getInt("companyFoundingYear");
//			String jsonRevenue = jsonObject.getString("companyRevenue");
//			String jsonTags = jsonObject.getString("tags");
//			String jsonNoteValue = jsonObject.getString("noteValue");
//			int jsonRating = jsonObject.getInt("rating");
//			int jsonScoreMaxPercentage= jsonObject.getInt("scoreMaxPercentage");
//			int jsonScoreAvg = jsonObject.getInt("scoreAvg");
//			int jsonVisits = jsonObject.getInt("visits");
//			String jsonVisitIdList = jsonObject.getString("visitIdList");
//			
//			cobusDataList.addRow(jsonCompany, jsonZip, jsonCity, jsonCountryCode, jsonBranch, jsonContacts, jsonPhone, jsonAddress, jsonCompanyId,
//					jsonDomain, jsonCompanySize, jsonFoundingYear, jsonRevenue, jsonTags, jsonNoteValue, jsonRating, jsonScoreMaxPercentage, 
//					jsonScoreAvg, jsonVisits, jsonVisitIdList);
//		}
		
		try { 
			JSONArray jsonArray = new JSONArray(jsonContent);
			
			int count = jsonArray.length(); // Anzahl der JsonObjekte
			for(int i=0 ; i < count; i++){   // Durch den JsonArray iterieren
				JSONObject jsonObject = jsonArray.getJSONObject(i);  // get jsonObject @ i position 
				String jsonCompany = jsonObject.getString("company");
				String jsonEmail = jsonObject.getString("email");
				
				boolean dublette = dublettenCheck(jsonObject, connection); // Überprüfen ob Firma bereits in Datenbank ist
				
				if(dublette == true) {
	                System.out.println("Die Firma " + jsonCompany + " gibt es bereits im System");
				}else {
					String callSpCobus = "{call dbo.cobus (?, ?)}";
					stmtInsertCompany = connection.prepareCall(callSpCobus);
					stmtInsertCompany.setString(1, jsonCompany);
					stmtInsertCompany.setString(2,  jsonEmail);
	                stmtInsertCompany.execute();
	                
					String callSpOrel = "{call dbo.cobusOrel (?)}";
					stmtInsertTableGuid = connection.prepareCall(callSpOrel);
					stmtInsertTableGuid.setString(1, jsonCompany);
	                stmtInsertTableGuid.execute();	
	                
	                String callSpChangeLog = "{call dbo.cobusChangeLog (?)}";
	                stmtInsertChangeLog = connection.prepareCall(callSpChangeLog);
	                stmtInsertChangeLog.setString(1, jsonCompany);
	                stmtInsertChangeLog.execute();
				}       
			}          
		}catch (JSONException e) {
			e.printStackTrace();
		}finally {
            if (selectStmtAddress0 != null) try { selectStmtAddress0.close(); } catch(Exception e) {}
            if (stmtInsertCompany != null) try { stmtInsertCompany.close(); } catch(Exception e) {}
            if (stmtInsertTableGuid != null) try { stmtInsertTableGuid.close(); } catch(Exception e) {}
            if (stmtInsertChangeLog != null) try { stmtInsertChangeLog.close(); } catch(Exception e) {}
            if (connection != null) try { connection.close(); } catch(Exception e){}
            System.out.println("Connection closed");
        }
	}
}
