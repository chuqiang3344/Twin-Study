package com.tyaer.spark_java.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperFactoryBean {

	private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static CuratorFramework zkClient;
    private String zkConnectionString;
    
    
    public static CuratorFramework createSimple(String connectionString){
    	// these are reasonable arguments for the ExponentialBackoffRetry. 
		// The first retry will wait 1 second - the second will wait up to 2 seconds - the
		// third will wait up to 4 seconds.
    	ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
    	CuratorFramework client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
    	client.start();
    	return client;
    }
    
    public static CuratorFramework createWithOptions(String connectionString, RetryPolicy retryPolicy, int connectionTimeoutMs, int sessionTimeoutMs) {
		// using the CuratorFrameworkFactory.builder() gives fine grained control
		// over creation options. See the CuratorFrameworkFactory.Builder javadoc details
    	if(null == zkClient){
	    	zkClient = CuratorFrameworkFactory.builder().connectString(connectionString)
					.retryPolicy(retryPolicy)
					.connectionTimeoutMs(connectionTimeoutMs)
					.sessionTimeoutMs(sessionTimeoutMs)
					// etc. etc.
					.build();
	    	zkClient.start();
    	}
    	return zkClient;
	}
    
}
