package com.datatec.sparker.utils.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;

/**
 * JDBC辅助组件
 * 
 * @author qianhao
 *
 */
public class JDBCHelper {

	private Logger logger = org.slf4j.LoggerFactory.getLogger(JDBCHelper.class);  
	static {
	}
	
	// 保证JDBCHelper只有一个实例，实例中只有一份数据库连接池
	private static JDBCHelper instance = null;
	
	/**
	 * 获取单例
	 * @return 单例
	 */
	public static JDBCHelper getInstance() {
		if(instance == null) {
			synchronized(JDBCHelper.class) {
				if(instance == null) {
					instance = new JDBCHelper();
				}
			}
		}
		return instance;
	}
	
	
	/**
	 * JDBCHelper在整个程序运行声明周期中，只会创建一次实例
	 * 在这一次创建实例的过程中，就会调用JDBCHelper()构造方法
	 * 此时，就可以在构造方法中，去创建自己唯一的一个数据库连接池
	 * 
	 */
	private JDBCHelper() {
	}
	
	/**
	 * 执行增删改SQL语句
	 * @param sql 
	 * @param params
	 * @return 影响的行数
	 * @throws Exception 
	 */
	public int executeUpdate(String sql, Object[] params,ConnectionBean connBean) throws Exception {
		int rtn = 0;
		PreparedStatement pstmt = null;
		
		try {
			Connection conn = connBean.getConnection();
			conn.setAutoCommit(false);  
			
			pstmt = conn.prepareStatement(sql);
			
			if(params != null && params.length > 0) {
				for(int i = 0; i < params.length; i++) {
					pstmt.setObject(i + 1, params[i]);  
				}
			}
			
			rtn = pstmt.executeUpdate();
			
			conn.commit();
		} catch (Exception e) {
			throw e; 
		} finally {
			
		}
		
		return rtn;
	}
	
	/**
	 * 执行SQL语句
	 * @param sql
	 * @param params
	 * @param callback
	 * @throws SQLException 
	 */
	public void executeSql(String sql,  ConnectionBean connBean) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			Connection conn = connBean.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.executeUpdate();
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(pstmt!=null){
				try {
					pstmt.close();
				} catch (SQLException e) {
					throw e;
				}
			}
		}
	}
	
	
	
	/**
	 * 批量执行SQL语句
	 * 
	 * @param sql
	 * @param paramsList
	 * @return 每条SQL语句影响的行数
	 * @throws Exception 
	 */
	public int[] executeBatch(String sql, List<List<Object>> paramsList, ConnectionBean connBean,int batchsize) throws Exception {
		int[] rtn = null;
		PreparedStatement pstmt = null;
		Connection conn = null;
		
		try {
			 conn = connBean.getConnection();
			
			// 第一步：使用Connection对象，取消自动提交
			conn.setAutoCommit(false);  
			
			pstmt = conn.prepareStatement(sql);
			// 第二步：使用PreparedStatement.addBatch()方法加入批量的SQL参数
			if(paramsList != null && paramsList.size() > 0) {
				int j = 1;
				for(List<Object> params : paramsList) {
					for(int i = 0; i < params.size(); i++) {
						//logger.info(params.get(i)+", ");
						String paramVal = null;
						if(params.get(i) != null){
							paramVal = String.valueOf(params.get(i));
						}
						pstmt.setString(i + 1, paramVal);  
					}
					pstmt.addBatch();
					//2W条提交一次
					if(j % batchsize == 0){
						pstmt.executeBatch();
						conn.commit();
					}
					j++;
				}
			}
			
			// 第三步：使用PreparedStatement.executeBatch()方法，执行批量的SQL语句
			rtn = pstmt.executeBatch();
			
			// 最后一步：使用Connection对象，提交批量的SQL语句
			conn.commit();
		} catch (Exception e) {
			throw e;  
		} finally {
			if(pstmt!=null){
				try {
					pstmt.close();
				} catch (SQLException e) {
					throw e;  
				}
			}
		}
		
		return rtn;
	}
	
	
	public void executeBatch(String sql, List<List<Object>> paramsList, List<ConnectionBean> connBeans,int batchsize) throws Exception {
		
		int connSize = connBeans.size();
		int dataSize = paramsList.size();
		
		int oneSize = dataSize / connSize;
		
		int k = 1;
        for (int i = 0; i < paramsList.size(); i+=oneSize) {
        	if (k == connSize) {        
        		oneSize = dataSize - i;
            }
            List<List<Object>> newList = paramsList.subList(i, i + oneSize);
            executeBatch(sql, newList, connBeans.get(k-1), batchsize);
            k++;
		}
		
	}
	
	/**
	 * 静态内部类：查询回调接口
	 * @author Administrator
	 *
	 */
	public static interface QueryCallback {
		
		/**
		 * 处理查询结果
		 * @param rs 
		 * @throws Exception
		 */
		void process(ResultSet rs) throws Exception;
		
	}
	
}
