package com.justsendit.awsrdshikaricpiam;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.rds.auth.RdsIamAuthTokenGenerator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class RdsDataSource {

    @Autowired
    private Environment env;

    // RDS IAM credentials expire every 15 minutes, so refresh every 14 minutes
    private static final int THREAD_SCHEDULE_TIME = 14;

    @Bean
    public DataSource getDataSource() throws Exception {
        String jdbcUrl = env.getProperty("spring.datasource.url");
        String dbUser = env.getProperty("spring.datasource.username");
        DataSource dataSource = this.createDataSource(jdbcUrl, dbUser);
        return dataSource;
    }

    /**
     * Creates a data source with the given jdbc url and jdbc username
     * the password is generated for RDS access and the connection uses SSL
     *
     * @param jdbcUrl the url of the data source
     * @param jdbcUsername the username to use to access the data source
     * @return the data source
     */
    private HikariDataSource createDataSource(String jdbcUrl, String jdbcUsername) throws Exception {
        RdsIamAuthTokenGenerator authTokenGenerator = RdsIamAuthTokenGenerator.builder()
        .credentials(new DefaultAWSCredentialsProviderChain())
        .region(env.getProperty("aws.region"))
        .build();
        
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(env.getProperty("spring.datasource.driver-class-name"));
        config.addDataSourceProperty("useSSL", true);
        config.addDataSourceProperty("requireSSL", true);
        config.addDataSourceProperty("verifyServerCertificate", true);
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(jdbcUsername);
        // set the initial password
        String password = DataSourceUtils.generateAuthToken(authTokenGenerator, config);
        config.setPassword(password);

        HikariDataSource hikariDataSource = new HikariDataSource(config);

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
                new IamWorker(hikariDataSource, authTokenGenerator),
                THREAD_SCHEDULE_TIME,   // wait x minutes for first execution
                THREAD_SCHEDULE_TIME,   // repeat the task every x minutes
                TimeUnit.MINUTES
        );

        return hikariDataSource;
    }

    /**
     * This runnable is used for generating IAM credentials using the aws rds sdk
     * and setting the password on the supplied data source to the new generated
     * token
     */
    private class IamWorker implements Runnable {

        HikariDataSource hikariDataSource;
        RdsIamAuthTokenGenerator tokenGenerator;

        IamWorker(HikariDataSource hikariDataSource, RdsIamAuthTokenGenerator tokenGenerator) {
            this.hikariDataSource = hikariDataSource;
            this.tokenGenerator = tokenGenerator;
        }

        @Override
        public void run() {
            System.out.println("Refreshing IAM credentials...");
            String password = DataSourceUtils.generateAuthToken(this.tokenGenerator, this.hikariDataSource);
            this.hikariDataSource.getHikariConfigMXBean().setPassword(password);
        }
    }
}