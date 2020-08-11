package connector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.Properties;

import org.postgresql.util.PSQLException;

import config.JoinConfig;
import config.LogConfig;

/**
 * Handles JDBC connection to database.
 * 
 * @author immanueltrummer
 *
 */
public class PgConnector {
	/**
	 * Connection to DBMS.
	 */
	public static Connection connection;
	/**
	 * Create connection to DBMS.
	 * 
	 * @param url			URL to database	
	 * @param user			user name
	 * @param password		password
	 * @throws Exception
	 */
	public static void connect(String url, String user, String password) 
	throws Exception {
		Properties props = new Properties();
		props.setProperty("user", user);
		props.setProperty("password", password);
		connection = DriverManager.getConnection(url, props);
		System.out.println("Established JDBC connection to " + url);
	}
	/**
	 * Disconnect from database.
	 * 
	 * @throws Exception
	 */
	public static void deconnect() throws Exception {
		connection.close();
		System.out.println("JDBC connection closed");
	}
	/**
	 * Executes an SQL statement and returns the result.
	 * 
	 * @param sql			SQL query string
	 * @return				query result set
	 * @throws Exception
	 */
	public static ResultSet query(String sql) throws Exception {
		if (LogConfig.VERBOSE) {
			System.out.println(sql);			
		}
		Statement statement = connection.createStatement();
		return statement.executeQuery(sql);
	}
	/**
	 * Executes and update SQL statement.
	 * 
	 * @param sql	SQL query string
	 * @return 		number of affected rows
	 * @throws Exception
	 */
	public static int update(String sql) throws Exception {
		if (LogConfig.VERBOSE) {
			System.out.println(sql);
		}
		Statement statement = connection.createStatement();
		return statement.executeUpdate(sql);
	}
	/**
	 * Executes an update with a timeout, fails gracefully
	 * upon timeout.
	 * 
	 * @param sql				SQL update query
	 * @param timeoutMillis		milliseconds until timeout
	 * @return	true iff update succeeds within timeout
	 * @throws Exception
	 */
	public static boolean updateOrTimeout(String sql, 
			int timeoutMillis) throws Exception {
		setTimeout(timeoutMillis);
		try {
			update(sql);
			return true;
		} catch (SQLTimeoutException e) {
        } catch (PSQLException | SQLRecoverableException e) {
            if (e.toString().contains("timeout") || 
            		e.toString().contains("timed out")) {
            } else {
            	throw e;
            }
		}
		return false;
	}
	/**
	 * Prints out Postgres plan for given statement.
	 * 
	 * @param sql			SQL query string
	 * @throws Exception
	 */
	public static void explain(String sql) throws Exception {
		ResultSet result = query("EXPLAIN " + sql);
		while (result.next()) {
			System.out.println(result.getString(1));
		}
	}
	/**
	 * Drops table of given name if the table exists.
	 * 
	 * @param tableName		name of table to drop
	 * @throws Exception
	 */
	public static void dropTable(String tableName) throws Exception {
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("DROP TABLE IF EXISTS ");
		sqlBuilder.append(tableName);
		sqlBuilder.append(";");
		update(sqlBuilder.toString());
	}
	/**
	 * Executes an update SQL statement with a timeout,
	 * returns true iff execution finishes within time.
	 * 
	 * @param sql			SQL query to execute
	 * @param timeoutMillis	timeout in milliseconds	
	 * @return		true iff execution finishes within budget
	 * @throws Exception
	 */
	/*
	public static boolean updateWithTimeout(String sql, 
			int timeoutMillis) throws Exception {
		connection.setAutoCommit(false);
		Statement statement = connection.createStatement();
		statement.executeUpdate("SET LOCAL statement_timeout = " + timeoutMillis);
		//setTimeout(timeoutMillis);
		boolean success = false;
		try {
			//PgConnector.update(sql);
			statement.executeUpdate(sql);
			connection.commit();
			if (LogConfig.VERBOSE) {
				System.out.println("No timeout.");				
			}
			success = true;
		} catch (SQLTimeoutException e) {
        } catch (PSQLException | SQLRecoverableException e) {
            if (e.toString().contains("timeout") || 
            		e.toString().contains("timed out")) {
            } else {
            	throw e;
            }
		}
		if (LogConfig.VERBOSE) {
			if (success) {
				System.out.println("No timeout.");
			} else {
				System.out.println("Had timeout!");
			}
		}
		//setNoTimeout();
		connection.setAutoCommit(true);
		return success;
	}
	*/
	/**
	 * Sets a timeout (in milliseconds) for the following statement.
	 * 
	 * @param millis		number of milliseconds until timeout
	 * @throws Exception
	 */
	public static void setTimeout(int millis) throws Exception {
		update("SET statement_timeout TO " + millis + ";");
	}
	/**
	 * Sets timeout to a very high value.
	 * 
	 * @throws Exception
	 */
	public static void setNoTimeout() throws Exception {
		boolean success = false;
		// Try resetting timeout until success (it can happen
		// that the reset command times out otherwise as we
		// are using very small timeouts).
		while (!success) {
			try {
				update("SET statement_timeout TO 1000000;");
				success = true;
			} catch (SQLTimeoutException e) {
	        } catch (PSQLException | SQLRecoverableException e) {
	            if (e.toString().contains("timeout") || 
	            		e.toString().contains("timed out")) {
	            } else {
	            	throw e;
	            }
			}
			if (!success) {
				System.out.println("Timeout while resetting timeout :-)");
			}
		}
	}
	/**
	 * Disables join order optimization by the original optimizer.
	 * 
	 * @throws Exception
	 */
	public static void disableJoinOrderOptimization() throws Exception {
		update("SET join_collapse_limit = 1;");
	}
	/**
	 * Enables join order optimization by the original optimizer.
	 * 
	 * @throws Exception
	 */
	public static void enableJoinOrderOptimization() throws Exception {
		update("SET join_collapse_limit = 12;");
	}
	/**
	 * Configure Postgres for batchwise execution.
	 */
	public static void enableBatchConfiguration() throws Exception {
		// Original optimizer may largely overestimate
		// number of rows in intermediate results -
		// avoid wrong operator choices.
		PgConnector.update("SET enable_material = false;");
		PgConnector.update("SET enable_mergejoin = false;");
		PgConnector.update("SET enable_hashjoin = false;");
		PgConnector.update("SET enable_nestloop = true;");
	}
	/**
	 * Configure Postgres for standard (i.e., non-batched) execution.
	 * @throws Exception
	 */
	public static void disableBatchConfiguration() throws Exception {
		PgConnector.update("SET enable_material = true;");
		PgConnector.update("SET enable_mergejoin = true;");
		PgConnector.update("SET enable_hashjoin = true;");
		PgConnector.update("SET enable_nestloop = " +
				(JoinConfig.enableNestLoopNonBatched?"true;":"false;"));
	}
}
