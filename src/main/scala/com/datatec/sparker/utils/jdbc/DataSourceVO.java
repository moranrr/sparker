package com.datatec.sparker.utils.jdbc;

import java.io.Serializable;

/**
 * 数据源对象
 * @author qianch
 *
 */
public class DataSourceVO implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2222543651092276178L;

	private String url;
	
	private String driver;
	
	private String userName;
	
	private String password;
	
	private int maxActive = 20;

	
	
	

	public DataSourceVO(String url, String driver, String userName,
			String password) {
		super();
		this.url = url;
		this.driver = driver;
		this.userName = userName;
		this.password = password;
	}

	

	public DataSourceVO(String url, String driver, String userName,
			String password, int maxActive) {
		super();
		this.url = url;
		this.driver = driver;
		this.userName = userName;
		this.password = password;
		this.maxActive = maxActive;
	}



	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public int getMaxActive() {
		return maxActive;
	}

	public void setMaxActive(int maxActive) {
		this.maxActive = maxActive;
	}



	public String getUserName() {
		return userName;
	}



	public void setUserName(String userName) {
		this.userName = userName;
	}



	public String getPassword() {
		return password;
	}



	public void setPassword(String password) {
		this.password = password;
	}
	
	
	
}
