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
	
	public static boolean getMyJSON(JSONObject jsonObject, ResultSet rs) throws SQLException{
		boolean isTrue = false;
			String company = jsonObject.getString("company");
			String email = jsonObject.getString("email");
			String jsonComp = company.toLowerCase();
			
			while(rs.next()) {
				String sqlCompName = rs.getString("COMPNAME");
				String sqlComp = sqlCompName.toLowerCase();
				if(sqlComp.equals(jsonComp)) {
					isTrue = true;
					break;
				}else {
					isTrue = false;
				}
			}
		return isTrue;
	}

	public static void main(String[] args) throws IOException, SQLException {
		
		URL url = new URL("http://www.json-generator.com/api/json/get/bUbmMVOGyG?indent=2");
		
		String sqlConnectionString;
		String compName;
		
		//JDBC Objekte
		Connection connection = null;
		Connection connectionOrel = null;
		ResultSet resultSet = null;
		ResultSet resultSetOrel = null;
		Statement selectStmt = null;
		Statement selectStmtOrel = null;
		CallableStatement stmt = null;
		CallableStatement stmtOrel = null;
		
		//config Datei einlesen
		File configFile = new File("C:\\Users\\CUH-GWX9\\git\\cobusJSON\\cobusJsonProjekt\\src\\cobusJsonProjekt\\config.properties");
		FileReader reader = new FileReader(configFile);
		Properties props = new Properties();
		props.load(reader);
		
		//connection String für die Verbindung zum SQL Server aufbauen
		sqlConnectionString = props.getProperty("general") + props.getProperty("database") + props.getProperty("user") + props.getProperty("password");
		
		//Verbindung zum SQL server herstellen
		connection = DriverManager.getConnection(sqlConnectionString);
		connectionOrel = DriverManager.getConnection(sqlConnectionString);
		System.out.println("Connected");
		
		//reading JSON-content via URL Connection & Inputstream
		URLConnection con = url.openConnection();
		InputStream in = con.getInputStream();
		String encoding = con.getContentEncoding();
		encoding = encoding == null ? "UTF-8" : encoding;
		String body = IOUtils.toString(in, encoding);	
		
		//JSONObject body beginnt ab '{' statt bei '['
		JSONObject jsonObj = new JSONObject(body.substring(body.indexOf('{')));
		
		//Select Statement aus der Config lesen
		String sqlSelect = props.getProperty("selectString");
        selectStmt = connection.createStatement();
        connection.setAutoCommit(false);
        
        //Select Statement aus der Config für die AddressOrel Tabelle auslesen
        String sqlSelectOrel = props.getProperty("selectStringOrel");	
        
        try {
			JSONArray jsonArray = new JSONArray(body);
			
			int count = jsonArray.length(); // get totalCount of all jsonObjects
			for(int i=0 ; i < count; i++){   // iterate through jsonArray 
				resultSet = selectStmt.executeQuery(sqlSelect);
				JSONObject jsonObject = jsonArray.getJSONObject(i);  // get jsonObject @ i position 
				String company = jsonObject.getString("company");
				String email = jsonObject.getString("email");
				boolean tempBoo = getMyJSON(jsonObject, resultSet);
				
		        String tableGuidSelect = "Select GGUID from ADDRESS0 where compname = '" + company +"'"; 
				
				if(tempBoo == true) {
	                System.out.println("Die Firma " + company + " gibt es bereits im System");
				}else {
					String callProc = "{call dbo.cobus (?, ?)}";
					stmt = connection.prepareCall(callProc);
					stmt.setString(1, company);
					stmt.setString(2,  email);
	                stmt.execute();
			        
				}
		        selectStmtOrel = connectionOrel.createStatement();
				resultSetOrel = selectStmtOrel.executeQuery(tableGuidSelect);
				
				if(!resultSetOrel.next()) {
					System.out.println("Das ResultSet ist leer");
				} else {
					String tableGuid = resultSetOrel.getString("GGUID");
			        tableGuid = "0x" + tableGuid;
					System.out.println(tableGuid);
					String callProcOrel = "{call dbo.cobusOrel (?)}";
					stmtOrel = connection.prepareCall(callProcOrel);
					stmtOrel.setString(1, tableGuid);
	                stmtOrel.execute();
				}
		        
			}
            
		}catch (JSONException e) {
			e.printStackTrace();
		}finally {
            if (resultSet != null) try { resultSet.close(); } catch(Exception e) {}  
            if (resultSetOrel != null) try { resultSetOrel.close(); } catch(Exception e) {}  
            if (selectStmt != null) try { selectStmt.close(); } catch(Exception e) {}
            if (selectStmtOrel != null) try { selectStmtOrel.close(); } catch(Exception e) {}
            if (stmt != null) try { stmt.close(); } catch(Exception e) {}
            if (connection != null) try { connection.close(); } catch(Exception e){}
            System.out.println("Connection closed");
        }
	}
}
