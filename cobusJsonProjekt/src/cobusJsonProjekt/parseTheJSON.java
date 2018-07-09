package cobusJsonProjekt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
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

	public static void main(String[] args) throws IOException {
		
		URL url = new URL("http://www.json-generator.com/api/json/get/bUbmMVOGyG?indent=2");
		
		//reading the JSON-content via URL Connection & Inputstream
		URLConnection con = url.openConnection();
		InputStream in = con.getInputStream();
		String encoding = con.getContentEncoding();
		encoding = encoding == null ? "UTF-8" : encoding;
		String body = IOUtils.toString(in, encoding);	
		
		//reading the json content via scanner
		Scanner scan = new Scanner(url.openStream());
		String str = new String();
		while(scan.hasNext())
			str += scan.nextLine();
		scan.close();
//		System.out.println(str);
		
		//JSONObject beginnt ab '{' statt bei '['
		JSONObject jsonObj = new JSONObject(body.substring(body.indexOf('{')));
		
		try {
			JSONArray jsonArray = new JSONArray(body);
 
			int count = jsonArray.length(); // get totalCount of all jsonObjects
			for(int i=0 ; i< count; i++){   // iterate through jsonArray 
				JSONObject jsonObject = jsonArray.getJSONObject(i);  // get jsonObject @ i position 
//				System.out.println("jsonObject " + i + ": " + jsonObject);
				String company = jsonObject.getString("company");
				System.out.println(company);
				String address = jsonObject.getString("address");
				System.out.println(address);
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
	}

}
