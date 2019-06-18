package com.datatec.sparker.utils.jdbc;

import java.sql.Connection;

public class ConnectionBean {
	private Connection connection; 
    private long updateTime;       
  
    public ConnectionBean(){
    	
    }
    
    
    public ConnectionBean(Connection connection, long updateTime) {
		super();
		this.connection = connection;
		this.updateTime = updateTime;
	}


	public Connection getConnection() {  
        return connection;  
    }  
  
    public void setConnection(Connection connection) {  
        this.connection = connection;  
    }  
  
    public long getUpdateTime() {  
        return updateTime;  
    }  
  
    public void setUpdateTime(long updateTime) {  
        this.updateTime = updateTime;  
    }


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (updateTime ^ (updateTime >>> 32));
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ConnectionBean other = (ConnectionBean) obj;
		if (updateTime != other.updateTime)
			return false;
		return true;
	} 
    
    
}
