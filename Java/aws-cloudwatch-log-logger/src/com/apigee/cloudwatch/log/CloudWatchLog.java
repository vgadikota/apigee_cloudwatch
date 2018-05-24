package com.apigee.cloudwatch.log;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.Date;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;



import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

public class CloudWatchLog {
	


	//private static final Logger log = LoggerFactory.getLogger(RequestSigner.class);

	private String acessKeyId;
	private String secretKey;
	private String groupName;
	private String streamName;
	private String region;

	private String lastSequenceToken;

	private boolean autoCreateLogGroup = true;
	private boolean autoCreateLogStream = true;

	public boolean put(String text) throws IOException {
		HttpURLConnection request;
		try {
			JsonObject root = new JsonObject();
			root.add("logGroupName", groupName).add("logStreamName", streamName).add("logEvents", new JsonArray()
					.add(new JsonObject().add("timestamp", System.currentTimeMillis()).add("message", text)));

			if (lastSequenceToken != null) {
				root.add("sequenceToken", lastSequenceToken);
			}
			byte[] bytes = root.toString().getBytes();

			request = execute(bytes, "PutLogEvents");

			String resp = getResponseString(request);

			//log.debug("Response: {}", resp);

			JsonObject obj = Json.parse(resp).asObject();
			JsonValue type = obj.get("__type");
			if (type != null && type.asString().equals("InvalidSequenceTokenException")) {
				//log.debug("Fixing sequenceToken");
				this.lastSequenceToken = obj.getString("expectedSequenceToken", null);
				return this.put(text);
			}
			if (type != null && type.asString().equals("ResourceNotFoundException")) {
				String message = obj.getString("message", "");
				if (message.toLowerCase().contains("group")) {
					//log.debug("Group not exists: {}", getGroupName());
					if (autoCreateLogGroup) {
						createGroup(getGroupName());
					}
				}
				if (message.toLowerCase().contains("stream")) {
					//log.debug("Stream not exists: {}", getStreamName());
					if (autoCreateLogStream) {
						createStream(getGroupName(), getStreamName());
					}
				}
				return this.put(text);
			}

			if (obj.get("nextSequenceToken") != null) {
				this.lastSequenceToken = obj.get("nextSequenceToken").asString();
			}

			if (request.getResponseCode() == 200) {
				//log.debug("Response OK=true -> {}", request.getResponseCode());
				return true;
			}
			//log.debug("Response OK=false -> {}", request.getResponseCode());
			return false;
		} catch (IOException e) {
			throw e;
		} finally {
			// if (request!=null) {
			// request.disconnect();
			// }
		}

	}

	private String getResponseString(HttpURLConnection request) throws IOException {
		String resp;
		if (request.getResponseCode() == 200) {
			InputStream inputStream = request.getInputStream();
			resp = convert(inputStream);
			inputStream.close();
		} else {
			InputStream inputStream = request.getErrorStream();
			resp = convert(inputStream);
			inputStream.close();
		}
		return resp;
	}

	private HttpURLConnection execute(byte[] bytes, String operation)
			throws MalformedURLException, IOException, ProtocolException {
		HttpURLConnection request;
		URL url = new URL("https://logs." + getRegion() + ".amazonaws.com");
		request = (HttpURLConnection) url.openConnection();
		request.setRequestMethod("POST");
		request.setRequestProperty("Content-type", "application/x-amz-json-1.1");
		request.setRequestProperty("X-Amz-Target", "Logs_20140328." + operation);

		RequestSigner v4RequestSigner = new RequestSigner(acessKeyId, secretKey, region, "logs", new Date());
		v4RequestSigner.signRequest(request, bytes);

		//log.debug("Request: {}", request.toString());

		request.setDoOutput(true);
		request.setDoInput(true);

		OutputStream outputStream = request.getOutputStream();
		outputStream.write(bytes);
		outputStream.close();
		
		
		// Testing with HTTP client
		
		/*try (CloseableHttpClient client = HttpClientBuilder.create().build()) {

            HttpPost request1 = new HttpPost("https://logs." + getRegion() + ".amazonaws.com");
            request1.setHeader("Content-type", "application/x-amz-json-1.1");
    		request1.setHeader("X-Amz-Target", "Logs_20140328." + operation);
            request1.setHeader("User-Agent", "Java client");
            request1.setEntity(new StringEntity("My test data"));
            
            RequestSigner v4RequestSigner1 = new RequestSigner(acessKeyId, secretKey, region, "logs", new Date());
    		v4RequestSigner1.signRequest(request1, bytes);

            HttpResponse response = client.execute(request1);

            BufferedReader bufReader = new BufferedReader(new InputStreamReader(
                    response.getEntity().getContent()));

            StringBuilder builder = new StringBuilder();

            String line;

            while ((line = bufReader.readLine()) != null) {
                builder.append(line);
                builder.append(System.lineSeparator());
            }

            System.out.println(builder);
        }*/
		
		
		
		
		
		return request;
		
		
		
	}

	@SuppressWarnings("resource")
	private String convert(InputStream is) {
		java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
		return s.hasNext() ? s.next() : "";
	}

	public void createStream(String groupName, String streamName) {
		JsonObject root = new JsonObject();
		root.add("logGroupName", groupName);
		root.add("logStreamName", streamName);

		try {
			HttpURLConnection conn = execute(root.toString().getBytes(), "CreateLogStream");
			String resp = getResponseString(conn);
			//log.debug("Response createStream: {}", resp);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void createGroup(String groupName) {
		JsonObject root = new JsonObject();
		root.add("logGroupName", groupName);

		try {
			HttpURLConnection conn = execute(root.toString().getBytes(), "CreateLogGroup");
			String resp = getResponseString(conn);
			//log.debug("Response createGroup: {}", resp);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String getAcessKeyId() {
		return acessKeyId;
	}

	public void setAcessKeyId(String acessKeyId) {
		this.acessKeyId = acessKeyId;
	}

	public String getSecretKey() {
		return secretKey;
	}

	public void setSecretKey(String secretKey) {
		this.secretKey = secretKey;
	}

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	public String getStreamName() {
		return streamName;
	}

	public void setStreamName(String streamName) {
		this.streamName = streamName;
	}

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public boolean isAutoCreateLogGroup() {
		return autoCreateLogGroup;
	}

	public void setAutoCreateLogGroup(boolean autoCreateLogGroup) {
		this.autoCreateLogGroup = autoCreateLogGroup;
	}

	public boolean isAutoCreateLogStream() {
		return autoCreateLogStream;
	}

	public void setAutoCreateLogStream(boolean autoCreateLogStream) {
		this.autoCreateLogStream = autoCreateLogStream;
	}

}
