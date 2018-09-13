package cobusJsonProjekt;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;

public class parseTheJSON {
	//Funktion nimmt JSONArray und String(Bezeichner) als Input und fügt die einzelnen Array Einträge zu einem String zusammen
	public static String stringCreator(JSONArray jArr) {
		String stringBuilder = "";
		int counter = jArr.length();
		for(int i = 0; i < counter; i++) {
			stringBuilder += jArr.getString(i);
			stringBuilder += ", \r\n";
		}
		return stringBuilder;
	}
	
	//Funktion nimmt Firmenname und Multimap mit Klickinformationen als Eingabe Parameter und ordnet die Links der besuchten Seite der entsprechenden
	//Firma zu
	public static String stringCreatorKlicks(String comp, Multimap<String, String> multi) {
		String klickUrls = "";
		
		Set<String> set = multi.keySet();
		Iterator<String> iterator = set.iterator();
		while(iterator.hasNext()) {
			String compName = (String) iterator.next();
			if(comp.equals(compName)) {
		    	 Collection<String> values = multi.get(compName);
		    	 Iterator<String> it = values.iterator();
		    	 while(it.hasNext()) {
		    		 klickUrls += it.next();
		    		 klickUrls += "\r\n";
		    	 }
		    }
		} 
	    return klickUrls;
	}
	
	public static void main(String[] args) throws IOException, SQLException {
			
		String sqlConnectionString;

		//JDBC Connection Objekt
		Connection connection = null;
		
		//config Datei aus dem home directory auslesen
		Properties props = new Properties();
		String home = System.getProperty("user.home");
		File userHome = new File(home, "config.properties");
		InputStream is = new FileInputStream(userHome);
		props.load(is);
		
		//connection String für die Verbindung zum SQL Server bilden
		sqlConnectionString = props.getProperty("general") + props.getProperty("database") + props.getProperty("user") + props.getProperty("password");
		
		//Verbindung zum SQL server herstellen
		connection = DriverManager.getConnection(sqlConnectionString);
		System.out.println("Connected");
		
		//JSON per URL auslesen
		URL url = new URL(props.getProperty("cobusUrl"));
		URLConnection con = url.openConnection();
		InputStream in = con.getInputStream();
		String encoding = con.getContentEncoding();
		encoding = encoding == null ? "UTF-8" : encoding;
		String jsonContent = IOUtils.toString(in, encoding);	
        
		SQLServerDataTable cobusDataList = new SQLServerDataTable(); //Tabelle mit Table Valued Parametern erstellen
		//Spaltenbezeichnung hinzufügen
		cobusDataList.addColumnMetadata("lfdnr", java.sql.Types.INTEGER);
		cobusDataList.addColumnMetadata("companyName", java.sql.Types.NVARCHAR);
		cobusDataList.addColumnMetadata("companyZip", java.sql.Types.NVARCHAR);
		cobusDataList.addColumnMetadata("companyCity", java.sql.Types.NVARCHAR);
		cobusDataList.addColumnMetadata("companyCountryCode", java.sql.Types.NVARCHAR);
		cobusDataList.addColumnMetadata("companyBranch", java.sql.Types.NVARCHAR);
		cobusDataList.addColumnMetadata("companyContacts", java.sql.Types.NVARCHAR);
		cobusDataList.addColumnMetadata("companyPhone", java.sql.Types.NVARCHAR);
		cobusDataList.addColumnMetadata("companyAddress", java.sql.Types.NVARCHAR);
		cobusDataList.addColumnMetadata("companyId", java.sql.Types.BIGINT);
		cobusDataList.addColumnMetadata("companyDomain", java.sql.Types.NVARCHAR);
		cobusDataList.addColumnMetadata("companySize", java.sql.Types.NVARCHAR);
		cobusDataList.addColumnMetadata("companyFoundingYear", java.sql.Types.NVARCHAR);
		cobusDataList.addColumnMetadata("companyRevenue", java.sql.Types.NVARCHAR);
		cobusDataList.addColumnMetadata("tags", java.sql.Types.NVARCHAR);
		cobusDataList.addColumnMetadata("noteValue", java.sql.Types.NVARCHAR);
		cobusDataList.addColumnMetadata("rating", java.sql.Types.SMALLINT);
		cobusDataList.addColumnMetadata("scoreMaxPercentage", java.sql.Types.TINYINT);
		cobusDataList.addColumnMetadata("scoreAvg", java.sql.Types.TINYINT);
		cobusDataList.addColumnMetadata("visits", java.sql.Types.INTEGER);
		cobusDataList.addColumnMetadata("visitIdList", java.sql.Types.NVARCHAR);
		cobusDataList.addColumnMetadata("visitedSite", java.sql.Types.NVARCHAR);
		
		JSONArray jsonArray = new JSONArray(jsonContent);
		JSONObject jsonObject = jsonArray.getJSONObject(0);  // jsonObject an erster Position 
		JSONObject dataSet = (JSONObject) jsonObject.get("dataSet"); // 1. Unterpunkt vom jsonObjekt
		JSONArray data = (JSONArray) dataSet.get("data"); // Array beinhaltet alle relevanten Daten -- hiermit wird gearbeitet
		
		//Klickinformationen für Kunden in Array speichern
		JSONObject klickKunden = jsonArray.getJSONObject(1);  //jsonObject an zweiter Position	
		JSONObject klickKundenDataSet = (JSONObject) klickKunden.get("dataSet");
		JSONArray klickKundenData = (JSONArray) klickKundenDataSet.get("data"); // Array beinhaltet alle Klickinformationen
		
		int countKlicks = klickKundenData.length();
		
		//Hashmap für Klickinformationen
		Multimap<String, String> klickMap = ArrayListMultimap.create();
	
		//for loop um die Klickinformationen + Firmennamen in Multimap zu speichern
		for(int i = 0; i < countKlicks; i++) {
			JSONObject finalDataKlickKunden = klickKundenData.getJSONObject(i);
			
			String klickKundenCompName = finalDataKlickKunden.getString("companyName");
			String besuchteSeiteUrl = finalDataKlickKunden.getString("pageUrlValue");
			
			klickMap.put(klickKundenCompName, besuchteSeiteUrl); // Multimap füllen
		}
		
		int lfdnr = 1;
		
		int count = data.length(); // Anzahl der JsonObjekte
		for(int i=0 ; i < count; i++){   // Durch den JsonArray iterieren

			JSONObject finalDataCollection = data.getJSONObject(i); // get jsonObject @ i position
			JSONArray jsonAddress = (JSONArray) finalDataCollection.get("companyAddress");
			
			//alle vorhandenen Informationen aus der JSON Datei in Variablen speichern
			String jsonCompany = finalDataCollection.getString("companyName");
			String jsonZip = finalDataCollection.getString("companyZip");
			String jsonCity = finalDataCollection.getString("companyCity");
			String jsonCountryCode = finalDataCollection.getString("companyCountryCode");
			String jsonBranch = finalDataCollection.getString("companyBranch");
			JSONArray arrayContacts = (JSONArray) finalDataCollection.get("companyContacts");
			String jsonContacts = stringCreator(arrayContacts);
			String jsonPhone = finalDataCollection.getString("companyPhone");
			String jsonStreet = jsonAddress.getString(0);
			long jsonCompanyId = finalDataCollection.getLong("companyId");
			String jsonDomain = finalDataCollection.getString("companyDomain");
			String jsonCompanySize = finalDataCollection.getString("companySize");
			String jsonFoundingYear = finalDataCollection.getString("companyFoundingYear");
			String jsonRevenue = finalDataCollection.getString("companyRevenue");
			JSONArray arrayTags = (JSONArray) finalDataCollection.get("tags");
			String jsonTags = stringCreator(arrayTags);
			String jsonNoteValue = finalDataCollection.getString("noteValue");
			int jsonRating = finalDataCollection.getInt("rating");
			int jsonScoreMaxPercentage= finalDataCollection.getInt("scoreMaxPercentage");
			int jsonScoreAvg = finalDataCollection.getInt("scoreAvg");
			int jsonVisits = finalDataCollection.getInt("visits");
			JSONArray arrayVisitIdList = (JSONArray) finalDataCollection.get("visitIdList");
			String jsonVisitIdList = stringCreator(arrayVisitIdList);
			String urls = stringCreatorKlicks(jsonCompany, klickMap);
			
			//JSON Daten in die SQLServerDataTable einfügen
			cobusDataList.addRow(lfdnr, jsonCompany, jsonZip, jsonCity, jsonCountryCode, jsonBranch, jsonContacts, jsonPhone, jsonStreet, jsonCompanyId,
					jsonDomain, jsonCompanySize, jsonFoundingYear, jsonRevenue, jsonTags, jsonNoteValue, jsonRating, jsonScoreMaxPercentage, 
					jsonScoreAvg, jsonVisits, jsonVisitIdList, urls);

			lfdnr++;
		}
		
		//insertData sp aufrufen und SQLServerDataTable übergeben, transferData sp aufrufen um die Daten in die richtigen Tabellen zu schreiben
		try {
			String ececSpTransferData = "EXEC dbo.TransferData";
			String ececStoredProc = "EXEC insertData ?";
			CallableStatement cStmt = connection.prepareCall(ececSpTransferData);
			SQLServerPreparedStatement pStmt = (SQLServerPreparedStatement)connection.prepareStatement(ececStoredProc);
			pStmt.setStructured(1, "dbo.DataTransferTypeCOBUS", cobusDataList); // Tabelle mit table valued Parametern an dbo.DataTransferTypeCOBUS übergeben
			pStmt.execute();			
			cStmt.execute();
		} catch (JSONException e) {
			e.printStackTrace();
		} catch(SQLServerException s) {
			s.printStackTrace();
		}
		finally {
            if (connection != null) try { connection.close(); } catch(Exception e){}
            System.out.println("Connection closed");
		}	
	}
}
