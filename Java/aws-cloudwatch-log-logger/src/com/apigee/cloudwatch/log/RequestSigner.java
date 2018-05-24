package com.apigee.cloudwatch.log;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.http.client.methods.HttpPost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class RequestSigner {
	
	

	//private static final Logger log = LoggerFactory.getLogger(RequestSigner.class);

	public static final String ENCODING = "UTF8";
	public static final String ISO_8601_TIME_FORMAT = "yyyyMMdd'T'HHmmss'Z'";
	public static final String ISO_8601_DATE_FORMAT = "yyyyMMdd";
	public static final String PAYLOAD_HASHING_ALGORITHM = "SHA-256";
	public static final String SIGN_STRING_ALGORITHM_NAME = "AWS4-HMAC-SHA256";
	public static final String SIGNATURE_HASHING_ALGORITHM = "HmacSHA256";
	public static final String DATE_HEADER_NAME = "x-amz-date";
	public static final String HOST_HEADER_NAME = "host";
	public static final String AUTH_HEADER_NAME = "Authorization";
	public static final String AUTH_HEADER_FORMAT = SIGN_STRING_ALGORITHM_NAME
			+ " Credential=%s/%s, SignedHeaders=%s, Signature=%s";
	public static final String SESSION_TOKEN_HEADER = "X-Amz-Security-Token";

	private final String regionName;
	private final String serviceName;
	private final Date currentTime;

	private String secretKey;

	private String accessKey;

	/**
	 * Test constructor with overriden date
	 */
	public RequestSigner(String accessKey, String secretKey, String regionName, String serviceName,
			Date currentTime) {
		this.regionName = regionName;
		this.serviceName = serviceName;
		this.secretKey = secretKey;
		this.currentTime = currentTime;
		this.accessKey = accessKey;
	}

	public void signRequest(HttpURLConnection request, byte[] content) {
		String canonicalRequest = createCanonicalRequest(request, content);
		//log.debug("canonicalRequest: {}" + canonicalRequest);
		String[] requestParts = canonicalRequest.split("\n");
		String signedHeaders = requestParts[requestParts.length - 2];
		String stringToSign = createStringToSign(canonicalRequest);
		//log.debug("stringToSign: {}" + stringToSign);
		String authScope = stringToSign.split("\n")[2];
		String signature = createSignature(stringToSign);

		String authHeader = String.format(AUTH_HEADER_FORMAT, this.accessKey, authScope, signedHeaders, signature);

		request.setRequestProperty(AUTH_HEADER_NAME, authHeader);
	}

	String createSignature(String stringToSign) {
		return toHexString(hmacSHA256(stringToSign, getSignatureKey()));
	}

	byte[] getSignatureKey() {
		byte[] secret = getBytes("AWS4" + this.secretKey);
		byte[] date = hmacSHA256(datestamp(), secret);
		byte[] retion = hmacSHA256(regionName, date);
		byte[] service = hmacSHA256(serviceName, retion);
		return hmacSHA256("aws4_request", service);
	}

	String createStringToSign(String canonicalRequest) {
		return SIGN_STRING_ALGORITHM_NAME + '\n' + timestamp() + '\n' + datestamp() + '/' + regionName + '/'
				+ serviceName + "/aws4_request\n"
				+ toHexString(sha256(new ByteArrayInputStream(getBytes(canonicalRequest))));
	}

	String createCanonicalRequest(HttpURLConnection request, byte[] content) {
		StringBuilder result = new StringBuilder();
		result.append(request.getRequestMethod()).append('\n');
		String path = request.getURL().getPath();
		if (path.isEmpty()) {
			path = "/";
		}
		result.append(path).append('\n');
		String queryString = getQuery(request);
		queryString = queryString != null ? queryString : "";
		addCanonicalQueryString(queryString, result).append('\n');
		addCanonicalHeaders(request, result).append('\n');

		try {
			addHashedPayload(new ByteArrayInputStream(content), result);
		} catch (IOException e) {
			throw new RuntimeException("Could not create hash for entity " + content, e);
		}
		return result.toString();
	}

	private String getQuery(HttpURLConnection request) {
		return null;
	}

	StringBuilder addCanonicalQueryString(String queryString, StringBuilder builder) {
		int startingLength = builder.length();
		SortedMap<String, String> encodedParams = new TreeMap<String, String>();
		for (String queryParam : queryString.split("&")) {
			if (!queryParam.isEmpty()) {
				String[] parts = queryParam.split("=", 2);
				encodedParams.put(encodeQueryStringValue(parts[0]), encodeQueryStringValue(parts[1]));
			}
		}
		for (Map.Entry<String, String> entry : encodedParams.entrySet()) {
			if (builder.length() > startingLength) {
				builder.append('&');
			}
			builder.append(entry.getKey()).append('=').append(entry.getValue());
		}
		return builder;
	}

	StringBuilder addCanonicalHeaders(HttpURLConnection request, StringBuilder builder) {
		SortedMap<String, String> sortedHeaders = sortedFormattedHeaders(request.getRequestProperties());
		if (!sortedHeaders.containsKey(DATE_HEADER_NAME)) {
			String timestamp = timestamp();
			sortedHeaders.put(DATE_HEADER_NAME, timestamp);
			request.setRequestProperty(DATE_HEADER_NAME, timestamp);
		}
		if (!sortedHeaders.containsKey(HOST_HEADER_NAME)) {
			sortedHeaders.put(HOST_HEADER_NAME, request.getURL().getHost());
			request.setRequestProperty(HOST_HEADER_NAME, request.getURL().getHost());
		}

		addCanonicalHeaders(sortedHeaders, builder).append('\n');
		return addSignedHeaders(sortedHeaders, builder);
	}

	SortedMap<String, String> sortedFormattedHeaders(Map<String, List<String>> headers) {
		SortedMap<String, String> sortedHeaders = new TreeMap<>();
		for (String header : headers.keySet()) {
			if (header != null) {
				sortedHeaders.put(header.toLowerCase(), headers.get(header).get(0).trim());
			}
		}
		return sortedHeaders;
	}

	StringBuilder addCanonicalHeaders(SortedMap<String, String> sortedFormattedHeaders, StringBuilder builder) {
		for (Map.Entry<String, String> entry : sortedFormattedHeaders.entrySet()) {
			builder.append(entry.getKey()).append(':').append(entry.getValue()).append('\n');
		}
		return builder;
	}

	StringBuilder addSignedHeaders(SortedMap<String, String> sortedFormattedHeaders, StringBuilder builder) {
		int startingLength = builder.length();
		for (String headerName : sortedFormattedHeaders.keySet()) {
			if (builder.length() > startingLength) {
				builder.append(';');
			}
			builder.append(headerName);
		}
		return builder;
	}

	StringBuilder addHashedPayload(InputStream payload, StringBuilder builder) throws IOException {
		return builder.append(toHexString(sha256(payload)));
	}

	String datestamp() {
		Date date = currentTime != null ? currentTime : Calendar.getInstance().getTime();
		DateFormat dateFormat = new SimpleDateFormat(ISO_8601_DATE_FORMAT);
		dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
		return dateFormat.format(date);
	}

	String timestamp() {
		Date date = currentTime != null ? currentTime : Calendar.getInstance().getTime();
		DateFormat dateFormat = new SimpleDateFormat(ISO_8601_TIME_FORMAT);
		dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
		return dateFormat.format(date);
	}

	String toHexString(byte[] data) {
		char[] hexChars = "0123456789abcdef".toCharArray();
		char[] result = new char[data.length * 2];
		for (int i = 0; i < data.length; i++) {
			int v = data[i] & 0xFF;
			result[i * 2] = hexChars[v >>> 4];
			result[i * 2 + 1] = hexChars[v & 0x0F];
		}
		return new String(result);
	}

	byte[] sha256(InputStream inputStream) {
		try {
			try {
				MessageDigest messageDigest = MessageDigest.getInstance(PAYLOAD_HASHING_ALGORITHM);
				BufferedInputStream bufferedStream = new BufferedInputStream(inputStream);
				byte[] buffer = new byte[16384];
				int bytesRead;
				while ((bytesRead = bufferedStream.read(buffer, 0, buffer.length)) != -1) {
					messageDigest.update(buffer, 0, bytesRead);
				}
				return messageDigest.digest();
			} finally {
				if (inputStream.markSupported()) {
					inputStream.reset();
				}
			}
		} catch (NoSuchAlgorithmException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	byte[] getBytes(String key) {
		try {
			return (key).getBytes(ENCODING);
		} catch (UnsupportedEncodingException e) {
			// Will never happen with "UTF8" hardcoded.
			throw new RuntimeException(e);
		}
	}

	/**
	 * RFC 3986 URI encoding, with substitution of the empty string for null
	 * values
	 */
	String encodeQueryStringValue(String s) {
		try {
			return URLEncoder.encode(s, ENCODING).replace("+", "%20").replace("*", "%2A").replace("%7E", "~");
		} catch (UnsupportedEncodingException e) {
			// Will never happen with "UTF8" hardcoded.
			return null;
		}
	}

	byte[] hmacSHA256(String data, byte[] key) {
		try {
			Mac mac = Mac.getInstance(SIGNATURE_HASHING_ALGORITHM);
			mac.init(new SecretKeySpec(key, SIGNATURE_HASHING_ALGORITHM));
			return mac.doFinal(getBytes(data));
		} catch (NoSuchAlgorithmException | InvalidKeyException e) {
			throw new RuntimeException(e);
		}
	}

	/*public void signRequest(HttpPost request1, byte[] content) {

		String canonicalRequest = createCanonicalRequest(request1, content);
		//log.debug("canonicalRequest: {}" + canonicalRequest);
		String[] requestParts = canonicalRequest.split("\n");
		String signedHeaders = requestParts[requestParts.length - 2];
		String stringToSign = createStringToSign(canonicalRequest);
		//log.debug("stringToSign: {}" + stringToSign);
		String authScope = stringToSign.split("\n")[2];
		String signature = createSignature(stringToSign);

		String authHeader = String.format(AUTH_HEADER_FORMAT, this.accessKey, authScope, signedHeaders, signature);

		request1.setHeader(AUTH_HEADER_NAME, authHeader);
	
		
	}

	private String createCanonicalRequest(HttpPost request, byte[] content) {
		// TODO Auto-generated method stub
		
		

		StringBuilder result = new StringBuilder();
		result.append(request.getMethod()).append('\n');
		String path = request.getURI().getPath();
		if (path.isEmpty()) {
			path = "/";
		}
		result.append(path).append('\n');
		String queryString = getQuery(request);
		queryString = queryString != null ? queryString : "";
		addCanonicalQueryString(queryString, result).append('\n');
		addCanonicalHeaders(request, result).append('\n');

		try {
			addHashedPayload(new ByteArrayInputStream(content), result);
		} catch (IOException e) {
			throw new RuntimeException("Could not create hash for entity " + content, e);
		}
		return result.toString();
	
	}

	private StringBuilder addCanonicalHeaders(HttpPost request, StringBuilder result) {
		// TODO Auto-generated method stub

		SortedMap<String, String> sortedHeaders = sortedFormattedHeaders(request.getRequestProperties());
		if (!sortedHeaders.containsKey(DATE_HEADER_NAME)) {
			String timestamp = timestamp();
			sortedHeaders.put(DATE_HEADER_NAME, timestamp);
			request.setRequestProperty(DATE_HEADER_NAME, timestamp);
		}
		if (!sortedHeaders.containsKey(HOST_HEADER_NAME)) {
			sortedHeaders.put(HOST_HEADER_NAME, request.getURL().getHost());
			request.setRequestProperty(HOST_HEADER_NAME, request.getURL().getHost());
		}

		addCanonicalHeaders(sortedHeaders, builder).append('\n');
		return addSignedHeaders(sortedHeaders, builder);
	
	}

	private String getQuery(HttpPost request) {
		// TODO Auto-generated method stub
		return null;
	}*/

}
