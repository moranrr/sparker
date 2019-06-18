package com.datatec.sparker.utils.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;


/**
 * 连接池
 * @author qianch
 */
public class MultiPoolHelper {
	private Logger logger = org.slf4j.LoggerFactory.getLogger(MultiPoolHelper.class);  
	private static long expirationTime;  
    private static MultiPoolHelper instance=null;
    private final static Map<String,DataSourceVO> DATASOURCE_MAP = new Hashtable<String,DataSourceVO>();
    private static Map<String, LinkedList<ConnectionBean>> activeConnectionMap = null;
    private static Map<String, LinkedList<ConnectionBean>> usingConnectionMap = null;
    
	
    private MultiPoolHelper(){
    	activeConnectionMap = new Hashtable<String, LinkedList<ConnectionBean>>();
        usingConnectionMap = new Hashtable<String, LinkedList<ConnectionBean>>();
        expirationTime = 4 * 60 * 60 * 1000; // 4xiaoshi 过期 
       
    }
    
    /**
     * 获取实例
     * @return
     */
    public static MultiPoolHelper getInstance(){
    	if(instance == null) {
			synchronized(MultiPoolHelper.class) {
				if(instance == null) {
					instance = new MultiPoolHelper();
				}
			}
		}
		return instance;
    }
    
    
    public synchronized ConnectionBean getConnection(String driver,String url,String userName,
			String password,int maxSize) throws InterruptedException, SQLException{
    	DataSourceVO dataSourceVO = DATASOURCE_MAP.get(url);
    	if(dataSourceVO == null){
    		dataSourceVO = new DataSourceVO(url, driver, userName, password, maxSize);
    		DATASOURCE_MAP.put(url, dataSourceVO);
    		init(dataSourceVO);
    	}
    	LinkedList<ConnectionBean> activeConnections = activeConnectionMap.get(url);
    	
    	ConnectionBean connection = getConnection(url, activeConnections);
    	int maxwait = 0;
    	
    	while(connection == null && maxwait < 10){
    		try {
				Thread.sleep(100);
				maxwait++;
			} catch (InterruptedException e) {
				throw e;
			}
    		connection = getConnection(url, activeConnections);
    	}
    	
    	return connection;
    }
    
    
    /**
     * 获取一个指定库的连接
     * @param dbType
     * @return
     * @throws SQLException 
     */
    private  ConnectionBean getConnection(String dbType,LinkedList<ConnectionBean> activeConnections) throws SQLException {
    	logger.info("【POOL】 getConnection, the DataSourceName is {}", "mysql"); 
    	
    	long now = System.currentTimeMillis();  
    	LinkedList<ConnectionBean> usingConnections = usingConnectionMap.get(dbType);
    	if(usingConnections == null){
    		usingConnections = new LinkedList<ConnectionBean>();
        	usingConnectionMap.put(dbType, usingConnections);
        }
    	
    	while(!activeConnections.isEmpty()){
    		ConnectionBean bean = activeConnections.poll(); 
    		if(bean == null){
    			continue;
    		}
    		// 如果头元素的时间过期了，那么关闭连接  
            if (now - bean.getUpdateTime() > expirationTime) {  
                logger.info("【POOL】 the connection is out of time ,bean time is {}", bean.getUpdateTime());  
                // 释放  
                expire(bean);  
                bean = null;  
            }else {  
                if (validate(bean)) {  
                    logger.info("【POOL】 get the connection from poll and the DataSourceName is {}",  
                    		"mysql");  
                    bean.setUpdateTime(now);  
                    // 放入正在使用的队列中并返回
                    usingConnections.add(bean);  
                    return bean;  
                } else {  
                    // 如果链接已经关闭  
                    bean = null;  
                }  
            }  
    	}
    	
    	//没有可用的连接池
    	for (Iterator<ConnectionBean> iterator = usingConnections.iterator(); iterator
				.hasNext();) {
			ConnectionBean connectionBean = (ConnectionBean) iterator.next();
			
			//如果连接已经使用了超过30min未归还
			if (connectionBean ==  null || now - connectionBean.getUpdateTime() >= 1800000) {
				expire(connectionBean); 
				connectionBean = null;  
				iterator.remove();
			}
		}
    	//如果已经创建的连接的个数没有超过配置的最大个数
    	DataSourceVO dataSourceVO = DATASOURCE_MAP.get(dbType);
    	if(dataSourceVO.getMaxActive() > usingConnections.size()){
    		Connection conn = createConnection(dataSourceVO.getDriver(), dataSourceVO.getUrl(),dataSourceVO.getUserName(), dataSourceVO.getPassword());
    		ConnectionBean connectionBean = new ConnectionBean(conn,System.currentTimeMillis()); 
    		usingConnections.add(connectionBean);
    		return connectionBean;
    	}
    	
    	return null;
	}
    
    /**
     * 释放连接
     * @param dbType
     * @param conn
     */
    public  void release(String dbType, ConnectionBean bean){
    	logger.info("【POOL】 release, the DataSourceName is {}", "mysql"); 
    	if(validate(bean)) {
    		activeConnectionMap.get(dbType).add(bean);
    		LinkedList<ConnectionBean> usingConnections = usingConnectionMap.get(dbType);
    		usingConnections.remove(bean);
		}
    }
    
    /**
     * 往连接池中增加一个连接
     * @param dbType
     * @param conn
     */
    public void add(String dbType, Connection conn){
    	LinkedList<ConnectionBean> activeConnections = activeConnectionMap.get(dbType) ;
        if(activeConnections == null){
        	activeConnections = new LinkedList<ConnectionBean>();
            activeConnectionMap.put(dbType, activeConnections);
        }
        ConnectionBean connectionBean = new ConnectionBean(conn,System.currentTimeMillis());
        activeConnections.add(connectionBean); //同时往空闲池增加一个连接
        
    }
    
    /***
     * 连接池是否活动
     * @return
     */
    public boolean isAlive(){
        if(activeConnectionMap.size() == 0) return false;
        return true;
    }
    
    public boolean validate(ConnectionBean o) {  
        try {  
            return (o != null && o.getConnection() != null && !(o.getConnection()).isClosed());  
        } catch (SQLException e) {  
            e.printStackTrace();  
            return false;  
        }  
    }
    
    
    private void expire(ConnectionBean o) throws SQLException {  
        try {  
            if(o != null && o.getConnection() != null) o.getConnection().close();  
        } catch (SQLException e) {  
           throw e; 
        }  
    } 
    
    /***
     * 初始化连接池
     * @param conns
     */
    public void init(DataSourceVO dataSourceVO){
    	//初始化
		for (int i = 0; i < dataSourceVO.getMaxActive(); i++) {
			Connection conn = createConnection(dataSourceVO.getDriver(), dataSourceVO.getUrl(), dataSourceVO.getUserName(), dataSourceVO.getPassword());
	        add(dataSourceVO.getUrl(), conn);
		}
		
    }
    
    /**
     * 连接数据库
     * @param driverClass
     * @param url
     * @param username
     * @param password
     * @return
     */
    private Connection createConnection(String driverClass, String url,String userName,
			String password){
        
        try {
            Class.forName(driverClass);
            Connection conn = null;
            if(StringUtils.isBlank(password) || StringUtils.isBlank(userName)){
            	conn = DriverManager.getConnection(url);
            }else{
            	conn = DriverManager.getConnection(url, userName, password);
            }
            System.out.println("connect success");
            return conn;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        } catch (ClassNotFoundException e1){
            e1.printStackTrace();
            return null;
        }
    }
    
}
