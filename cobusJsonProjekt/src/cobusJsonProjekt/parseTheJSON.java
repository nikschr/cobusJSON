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
import java.sql.SQLException;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.microsoft.sqlserver.jdbc.SQLServerDataTable;

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
	
	public static String stringCreator(JSONArray jArr, String name) {
		String stringBuilder = name + ": ";
		int counter = jArr.length();
		for(int i = 0; i < counter; i++) {
			stringBuilder += jArr.getString(i);
			stringBuilder = stringBuilder + ", ";
		}
		return stringBuilder;
	}
	
	public static void main(String[] args) throws IOException, SQLException {
		
		URL url = new URL("https://comatra2.cobus-concept.de/app.php/feedreports/configs/1528791151589650420/feeds/229597e4efec158142537bdf77af80b22b91714d");	
		String sqlConnectionString;

		//JDBC Connection Objekt
		Connection connection = null;
		
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
        
		SQLServerDataTable cobusDataList = new SQLServerDataTable(); //Tabelle mit Table Value paaren erstellen
		//Spaltenbezeichnung hinzufügen
		cobusDataList.addColumnMetadata("laufendeNr", java.sql.Types.INTEGER);
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

		JSONArray jsonArray = new JSONArray(jsonContent);
		JSONObject jsonObject = jsonArray.getJSONObject(0);  // get jsonObject @ i position 
		JSONObject dataSet = (JSONObject) jsonObject.get("dataSet");
		JSONArray data = (JSONArray) dataSet.get("data");
		
		int lfdnr = 1;
		
		int count = data.length(); // Anzahl der JsonObjekte
		for(int i=0 ; i < count; i++){   // Durch den JsonArray iterieren

			JSONObject finalDataCollection = data.getJSONObject(i);
			JSONArray jsonAddress = (JSONArray) finalDataCollection.get("companyAddress");
			
			//alle vorhandenen Informationen aus der JSON Datei in Variablen speichern
			String jsonCompany = finalDataCollection.getString("companyName");
			String jsonZip = finalDataCollection.getString("companyZip");
			String jsonCity = finalDataCollection.getString("companyCity");
			String jsonCountryCode = finalDataCollection.getString("companyCountryCode");
			String jsonBranch = finalDataCollection.getString("companyBranch");
			JSONArray arrayContacts = (JSONArray) finalDataCollection.get("companyContacts");
			String jsonContacts = stringCreator(arrayContacts, "Kontakte");
			String jsonPhone = finalDataCollection.getString("companyPhone");
			String jsonStreet = jsonAddress.getString(0);
			long jsonCompanyId = finalDataCollection.getLong("companyId");
			String jsonDomain = finalDataCollection.getString("companyDomain");
			String jsonCompanySize = finalDataCollection.getString("companySize");
			String jsonFoundingYear = finalDataCollection.getString("companyFoundingYear");
			String jsonRevenue = finalDataCollection.getString("companyRevenue");
			JSONArray arrayTags = (JSONArray) finalDataCollection.get("tags");
			String jsonTags = stringCreator(arrayTags, "Tags");
			String jsonNoteValue = finalDataCollection.getString("noteValue");
			int jsonRating = finalDataCollection.getInt("rating");
			int jsonScoreMaxPercentage= finalDataCollection.getInt("scoreMaxPercentage");
			int jsonScoreAvg = finalDataCollection.getInt("scoreAvg");
			int jsonVisits = finalDataCollection.getInt("visits");
			JSONArray arrayVisitIdList = (JSONArray) finalDataCollection.get("visitIdList");
			String jsonVisitIdList = stringCreator(arrayVisitIdList, "VistIdList");
			
//			System.out.println(jsonContacts);
			
			//JSON Daten in die SQLServerDataTable einfügen
			cobusDataList.addRow(lfdnr, jsonCompany, jsonZip, jsonCity, jsonCountryCode, jsonBranch, jsonContacts, jsonPhone, jsonStreet, jsonCompanyId,
					jsonDomain, jsonCompanySize, jsonFoundingYear, jsonRevenue, jsonTags, jsonNoteValue, jsonRating, jsonScoreMaxPercentage, 
					jsonScoreAvg, jsonVisits, jsonVisitIdList);

			lfdnr++;
		}
		//insertData sp aufrufen und SQLServerDataTable übergeben
//		try {
//			String ececStoredProc = "EXEC insertData ?";
//			SQLServerPreparedStatement pStmt = (SQLServerPreparedStatement)connection.prepareStatement(ececStoredProc);
//			pStmt.setStructured(1, "dbo.DataTransferTypeCOBUS", cobusDataList);
//			pStmt.execute();			
//		} catch (JSONException e) {
//			e.printStackTrace();
//		}finally {
//            if (connection != null) try { connection.close(); } catch(Exception e){}
//            System.out.println("Connection closed");
//		}
		
		
	}
}
