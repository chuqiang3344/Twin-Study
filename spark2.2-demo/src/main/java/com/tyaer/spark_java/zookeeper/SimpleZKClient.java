package com.tyaer.spark_java.zookeeper;

import com.tyaer.spark_java.Config;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/**
 * get CuratorFramework instance by singleton mode.
 *
 */
public class SimpleZKClient {
	
	private static Map<String, String> configs;
	private static volatile CuratorFramework zkClient = null;
	
	private SimpleZKClient() {
		
	}
	static{
		try {
			initCfg();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public static CuratorFramework getZKClient() throws IOException{
		if(null==zkClient){
			synchronized (SimpleZKClient.class) {
				if(null == zkClient){
					String connectionString = configs.get(ZK_LOCATION);
					int baseSleepTimeMs = Integer.valueOf(configs.get(ZK_BASE_SLEEP_TIME)).intValue();
					int maxRetries = Integer.valueOf(configs.get(ZK_MAX_RETRIES)).intValue();
					int connectionTimeoutMs = Integer.valueOf(configs.get(ZK_CONNECTION_TIMEOUT)).intValue();
					int sessionTimeoutMs = Integer.valueOf(configs.get(ZK_SESSION_TIMEOUT)).intValue();
					zkClient = ZookeeperFactoryBean.createWithOptions(connectionString, 
							new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries), connectionTimeoutMs, sessionTimeoutMs);
				}
			}
		}
		
		return zkClient;
	}
	
	public  static Map<String, String> getCfg(){
		return configs;
	}
	
	public static Map<String, String> initCfg() throws IOException{
		 Properties pps = new Properties();
		 pps.load(Config.class.getResourceAsStream(ZK_CONFIG_FILE));
		configs= new HashMap<String,String>();
		configs.put(ZK_BASE_SLEEP_TIME,pps.getProperty(ZK_BASE_SLEEP_TIME));
		configs.put(ZK_MAX_RETRIES,pps.getProperty(ZK_MAX_RETRIES));
		configs.put(ZK_CONNECTION_TIMEOUT,pps.getProperty(ZK_CONNECTION_TIMEOUT));
		configs.put(ZK_SESSION_TIMEOUT,pps.getProperty(ZK_SESSION_TIMEOUT));
		configs.put(ZK_LOCATION,pps.getProperty(ZK_LOCATION));
		configs.put(ZK_KAFKA_OFFSET_PATH,pps.getProperty(ZK_KAFKA_OFFSET_PATH));
		return configs;
	}
	
	
    private static final String ZK_CONFIG_FILE = "/zk/zk-config.properties"; 
	//private static final String ZK_CONFIG_FILE ="C:\\Users\\izhonghong\\scala\\spark-sample\\src.main.resources\\zk\\zk-config.properties";
	private static final String ZK_BASE_SLEEP_TIME = "zk.baseSleepTimeMs"; 
	private static final String ZK_MAX_RETRIES = "zk.maxRetries"; 
	private static final String ZK_CONNECTION_TIMEOUT = "zk.connectionTimeoutMs"; 
	private static final String ZK_SESSION_TIMEOUT = "zk.sessionTimeoutMs"; 
	public static final String ZK_LOCATION = "zk.location"; 
	public static final String ZK_KAFKA_OFFSET_PATH = "zk.kafkaOffsetPath"; 

}
