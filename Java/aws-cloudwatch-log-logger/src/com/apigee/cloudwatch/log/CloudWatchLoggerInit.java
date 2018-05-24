package com.apigee.cloudwatch.log;

import java.io.IOException;

import org.apache.commons.lang3.exception.ExceptionUtils;

import com.apigee.flow.execution.ExecutionContext;
import com.apigee.flow.execution.ExecutionResult;
import com.apigee.flow.execution.spi.Execution;
import com.apigee.flow.message.Message;
import com.apigee.flow.message.MessageContext;
import com.eclipsesource.json.JsonObject;

public class CloudWatchLoggerInit implements Execution{

	/*public static void main(String[] args) {
		CloudWatchLog cloudWatchLog = new CloudWatchLog();
		cloudWatchLog.setGroupName("Apigee-demo-group-1");
		cloudWatchLog.setAcessKeyId("AKIAJ5N4MTHI53DSQXVQ");
		cloudWatchLog.setSecretKey("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");
		cloudWatchLog.setRegion("us-east-2");
		cloudWatchLog.setStreamName("Apigee-demo-stream-1");
		
		JsonObject root = new JsonObject();
		root.add("organization", "amer-poc12");
		root.add("environment", "test");
		root.add("apiProduct", "Hello");
		root.add("proxyName", "payment-v1");
		root.add("appName", "Hello");
		root.add("verb", "POST");
		root.add("url", "https://amer-poc12-prod.apigee.net/guardian/v1/payments");
		root.add("responseReason", "OK");
		root.add("clientLatency", 3);
		root.add("targetLatency", 3);
		root.add("totalLatency", 3);
		root.add("pathsuffix", "/payments");
		root.add("environment.name", "test");
		root.add("apiproxy.revision", "5");
		root.add("apigee.client_id", "null");
		root.add("apigee.developer.app.name", "null");
		root.add("request.header.X-Forwarded-For", "null");
		root.add("client.received.start.timestamp", System.currentTimeMillis());
		root.add("client.sent.end.timestamp", System.currentTimeMillis());
		root.add("message.status.code", "200");
		root.add("response.status.code", "200");
		root.add("request.header.Accept", "application/json");
		root.add("request.queryparam.tokenValidityInMin", "3600");
		root.add("request.header.contentLength", "20");
		root.add("request.queryparam.correlationId", "ACMEXAWQE123409876");
		
		System.out.println(root.toString());
		try {
			cloudWatchLog.put(root.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}*/

	@Override
	public ExecutionResult execute(MessageContext msgCtxt, ExecutionContext execCtxt) {

        try {
            Message msg = msgCtxt.getMessage();
            String msgContent = msg.getContent();
            
            // calculate response times for client, target and total
            
            long request_start_time = 0;
            if(null!=msgCtxt.getVariable("client.received.start.timestamp")) {
            	request_start_time = Long.parseLong(msgCtxt.getVariable("client.received.start.timestamp").toString());
            }
            
            long target_start_time = 0;
            if(null!=msgCtxt.getVariable("target.sent.start.timestamp")) {
            	target_start_time = Long.parseLong(msgCtxt.getVariable("target.sent.start.timestamp").toString());
            }
            
            long target_end_time = 0;
            if(null!=msgCtxt.getVariable("target.received.end.timestamp")) {
            	target_end_time = Long.parseLong(msgCtxt.getVariable("target.received.end.timestamp").toString());
            }
           
            long request_end_time = 0;
            if(null!=msgCtxt.getVariable("system.timestamp")) {
            	request_end_time = Long.parseLong(msgCtxt.getVariable("system.timestamp").toString());
            }else {
            	request_end_time = System.currentTimeMillis();
            }
            
           
            long total_request_time = request_end_time-request_start_time;
            long total_target_time  = target_end_time-target_start_time;
            long total_client_time  = total_request_time-total_target_time;
            
          /*  CloudWatchLog cloudWatchLog = new CloudWatchLog();
    		cloudWatchLog.setGroupName("Apigee-demo-group-1"msgCtxt.getVariable("private.AWS_GROUP_NAME").toString());
    		cloudWatchLog.setAcessKeyId("AKIAJ5N4MTHI53DSQXVQ"msgCtxt.getVariable("private.AWS_KEY_1").toString());
    		cloudWatchLog.setSecretKey("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"msgCtxt.getVariable("private.AWS_SECRET").toString());
    		cloudWatchLog.setRegion("us-east-2"msgCtxt.getVariable("private.AWS_REGION_NAME").toString());
    		cloudWatchLog.setStreamName("Apigee-demo-stream-1"msgCtxt.getVariable("private.AWS_STREAM_NAME").toString());*/
    		
    		CloudWatchLog cloudWatchLog = new CloudWatchLog();
    		cloudWatchLog.setGroupName("Apigee-demo-group-1");
    		cloudWatchLog.setAcessKeyId("AKIAJ5N4MTHI53DSQXVQ");
    		cloudWatchLog.setSecretKey("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");
    		cloudWatchLog.setRegion("us-east-2");
    		cloudWatchLog.setStreamName("Apigee-demo-stream-1");
    		
    		JsonObject root = new JsonObject();
    		if(null!=msgCtxt.getVariable("organization.name")) {
    		root.add("organization", msgCtxt.getVariable("organization.name").toString());
    		}else {
    			root.add("organization", "null");
    		}
    		if(null!=msgCtxt.getVariable("environment.name")) {
    		root.add("environment", msgCtxt.getVariable("environment.name").toString());
    		}else {
    			root.add("environment", "null");
    		}
    		
    		if(null!=msgCtxt.getVariable("apiproduct.name")) {
    		root.add("apiProduct", msgCtxt.getVariable("apiproduct.name").toString());
    		}else {
    			root.add("apiProduct", "null");
    		}
    		
    		if(null!=msgCtxt.getVariable("apiproxy.name")) {
    		root.add("proxyName", msgCtxt.getVariable("apiproxy.name").toString());
    		}else {
    			root.add("proxyName", "null");
    		}
    		
    		if(null!=msgCtxt.getVariable("developer.app.name")) {
    		root.add("appName", msgCtxt.getVariable("developer.app.name").toString());
    		}else {
    			root.add("appName", "null");
    		}
    		
    		if(null!=msgCtxt.getVariable("request.verb")) {
    		root.add("verb", msgCtxt.getVariable("request.verb").toString());
    		}else {
    			root.add("verb", "null");
    		}
    		
    		if(null!=msgCtxt.getVariable("client.scheme") && null!=msgCtxt.getVariable("request.header.host") && null!=msgCtxt.getVariable("request.uri")) {
    		root.add("url", msgCtxt.getVariable("client.scheme") + "://" + msgCtxt.getVariable("request.header.host").toString() + msgCtxt.getVariable("request.uri").toString());
    		}else {
    			root.add("url", "null");
    		}
    		
    		if(null!=msgCtxt.getVariable("message.reason.phrase")) {
    		root.add("responseReason", msgCtxt.getVariable("message.reason.phrase").toString());
    		}else {
    			root.add("responseReason", "null");
    		}
    		
    		
    		root.add("clientLatency",total_client_time);
    		root.add("targetLatency",total_target_time);
    		root.add("totalLatency", total_request_time);
    		
    		if(null!=msgCtxt.getVariable("proxy.pathsuffix")) {
    		root.add("pathsuffix", msgCtxt.getVariable("proxy.pathsuffix").toString());
    		}else {
    			root.add("pathsuffix", "null");
    		}
    		
    		if(null!=msgCtxt.getVariable("environment.name")) {
    		root.add("environment.name", msgCtxt.getVariable("environment.name").toString());
    		}else {
    			root.add("environment.name", "null");
    		}
    		
    		if(null!=msgCtxt.getVariable("apiproxy.revision")){
    		root.add("apiproxy.revision", msgCtxt.getVariable("apiproxy.revision").toString());
    		}else {
    			root.add("apiproxy.revision", "null");
    		}
    		
    		if(null!=msgCtxt.getVariable("apigee.client_id")){
    		root.add("apigee.client_id", msgCtxt.getVariable("apigee.client_id").toString());
    		}else {
    			root.add("apigee.client_id", "null");
    		}
    		
    		if(null!=msgCtxt.getVariable("apigee.developer.app.name")) {
    		root.add("apigee.developer.app.name", msgCtxt.getVariable("apigee.developer.app.name").toString());
    		}else {
    			root.add("apigee.developer.app.name", "null");
    		}
    		
    		if(null!=msgCtxt.getVariable("request.header.X-Forwarded-For")){
    		root.add("request.header.X-Forwarded-For", msgCtxt.getVariable("request.header.X-Forwarded-For").toString());
    		}else {
    			root.add("request.header.X-Forwarded-For", "null");
    		}
    		
    		if(null!=msgCtxt.getVariable("client.received.start.timestamp")) {
    		root.add("client.received.start.timestamp", msgCtxt.getVariable("client.received.start.timestamp").toString());
    		}else {
    			root.add("client.received.start.timestamp", "null");
    		}
    		
    		if(null!=msgCtxt.getVariable("client.sent.end.timestamp")) {
    		root.add("client.sent.end.timestamp", String.valueOf(request_end_time)/*msgCtxt.getVariable("client.sent.end.timestamp").toString()*/);
    		}else {
    			root.add("client.sent.end.timestamp", "null");
    		}
    		
    		if(null!=msgCtxt.getVariable("message.status.code")) {
    		root.add("message.status.code", msgCtxt.getVariable("message.status.code").toString());
    		}else {
    			root.add("message.status.code", "null");
    		}
    		
    		if(null!=msgCtxt.getVariable("response.status.code")) {
    		root.add("response.status.code", msgCtxt.getVariable("response.status.code").toString());
    		}else {
    			root.add("response.status.code", "null");
    		}
    		
    		if(null!=msgCtxt.getVariable("request.header.Accept")) {
    		root.add("request.header.Accept", msgCtxt.getVariable("request.header.Accept").toString());
    		}else {
    			root.add("request.header.Accept", "null");
    		}
    		
    		if(null!=msgCtxt.getVariable("request.queryparam.tokenValidityInMin")) {
    		root.add("request.queryparam.tokenValidityInMin", msgCtxt.getVariable("request.queryparam.tokenValidityInMin").toString());
    		}else {
    			root.add("request.queryparam.tokenValidityInMin", "null");
    		}
    		
    		if(null!=msgCtxt.getVariable("request.header.contentLength")) {
    		root.add("request.header.contentLength", msgCtxt.getVariable("request.header.contentLength").toString());
    		}else {
    			root.add("request.header.contentLength", "null");
    		}
    		
    		if(null!=msgCtxt.getVariable("request.queryparam.correlationId")) {
    		root.add("request.queryparam.correlationId", msgCtxt.getVariable("request.queryparam.correlationId").toString());
    		}else {
    			root.add("request.queryparam.correlationId", "null");
    		}
    		
    		
    		
    		
    		msgCtxt.setVariable("message.log.content", root);
    		//System.out.println(root.toString());
    		cloudWatchLog.put(root.toString());
            
        }
        catch (Exception e) {
            //System.out.println(ExceptionUtils.getStackTrace(e));
            
            String error = e.getCause().toString();
            msgCtxt.setVariable("cloudwatch_exception", error);
            int ch = error.lastIndexOf(':');
            if (ch >= 0) {
                msgCtxt.setVariable("cloudwatch_exception", error.substring(ch+2).trim());
            }
            else {
                msgCtxt.setVariable("cloudwatch_exception", error);
            }
            msgCtxt.setVariable("cloudwatch_stacktrace", ExceptionUtils.getStackTrace(e));
            return ExecutionResult.ABORT;
        }

        return ExecutionResult.SUCCESS;
    }

}
