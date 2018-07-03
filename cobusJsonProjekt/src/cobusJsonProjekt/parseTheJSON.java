package cobusJsonProjekt;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.io.IOUtils;

public class parseTheJSON {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		URL url = new URL("http://www.json-generator.com/api/json/get/bUbmMVOGyG?indent=2");
		URLConnection con = url.openConnection();
		InputStream in = con.getInputStream();
		String encoding = con.getContentEncoding();
		encoding = encoding == null ? "UTF-8" : encoding;
		String body = IOUtils.toString(in, encoding);
		System.out.println(body);
		
		
	}

}
