package com.justsendit.awsrdshikaricpiam;

import java.net.URI;

import com.amazonaws.services.rds.auth.GetIamAuthTokenRequest;
import com.amazonaws.services.rds.auth.RdsIamAuthTokenGenerator;
import com.zaxxer.hikari.HikariConfig;

public class DataSourceUtils {
    
    public static String generateAuthToken(RdsIamAuthTokenGenerator authTokenGenerator, HikariConfig config) {
        String url = config.getJdbcUrl();
        String cleanURI = url.substring(5);

        URI uri = URI.create(cleanURI);
        String authToken = authTokenGenerator.getAuthToken(
		    GetIamAuthTokenRequest.builder()
		    .hostname(uri.getHost())
		    .port(uri.getPort())
		    .userName(config.getUsername())
		    .build());
	    
	    return authToken;
    }
    
}