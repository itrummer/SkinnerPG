package catalog;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import config.NamingConfig;
import connector.PgConnector;
import query.ColumnRef;

/**
 * Utility functions for accessing Postgres
 * database catalog. A database connection
 * must have been established beforehand.
 * More generally: functions for retrieving
 * information on database tables.
 * 
 * @author immanueltrummer
 *
 */
public class PgCatalog {
	/**
	 * Executes an SQL query that returns a single integer, 
	 * returns result.
	 * 
	 * @param sql	SQL query to execute
	 * @return		single integer query result
	 * @throws Exception
	 */
	static int queryForInt(String sql) throws Exception {
		ResultSet result = PgConnector.query(sql);
		result.next();
		return result.getInt(1);
	}
	/**
	 * Retrieves cardinality for given table.
	 * 
	 * @param tableName		name of table whose cardinality is retrieved
	 * @return				cardinality value
	 * @throws Exception
	 */
	public static int cardinality(String tableName) throws Exception {
		return queryForInt("SELECT COUNT(*) FROM " + tableName + ";");
	}
	/**
	 * Retrieves minimal disk page ID among all table pages.
	 * 
	 * @param tableName	consider pages of this table
	 * @return			minimum disk page ID
	 * @throws Exception
	 */
	public static int minPage(String tableName) throws Exception {
		return queryForInt("SELECT MIN((ctid::text::point)[0]::int) FROM " + tableName + ";");
	}
	/**
	 * Retrieves maximal disk page ID among all table pages.
	 * 
	 * @param tableName	consider pages of this table
	 * @return			minimum disk page ID
	 * @throws Exception
	 */
	public static int maxPage(String tableName) throws Exception {
		return queryForInt("SELECT MAX((ctid::text::point)[0]::int) FROM " + tableName + ";");
	}
	/**
	 * Returns information on columns of a given table.
	 * 
	 * @param tableName		name of table whose columns are returned
	 * @return				list of objects representing column meta-data
	 * @throws Exception
	 */
	public static List<ColumnInfo> columnMeta(String tableName) throws Exception {
		List<ColumnInfo> columns = new ArrayList<ColumnInfo>();
		String sql = "SELECT column_name, data_type " + 
				"FROM information_schema.columns " + 
				"WHERE table_name = '" + tableName + "'";
		ResultSet result = PgConnector.query(sql);
		while (result.next()) {
			String columnName = result.getString(1);
			String columnType = result.getString(2);
			columns.add(new ColumnInfo(tableName, columnName, columnType));
		}
		return columns;
	}
	/**
	 * Returns names of all public base tables in current database.
	 * 
	 * @return	list of table names
	 * @throws Exception
	 */
	public static List<String> baseTables() throws Exception {
		List<String> tableNames = new ArrayList<String>();
		// Query for tables but exclude Skinner's internal tables
		ResultSet result = PgConnector.query(
				"SELECT table_name FROM information_schema.tables " +
						"WHERE table_schema = 'public' AND " +
						"table_name NOT LIKE '" + NamingConfig.SKINNER_PREFIX + "%';");
		while (result.next()) {
			tableNames.add(result.getString(1));
		}
		return tableNames;
	}
	/**
	 * Returns names of all columns on which constraints are placed,
	 * except for columns in Skinner-internal tables.
	 * 
	 * @return	list of column references
	 * @throws Exception
	 */
	public static List<ColumnRef> constrainedColumns() throws Exception {
		List<ColumnRef> columns = new ArrayList<ColumnRef>();
		ResultSet result = PgConnector.query(
				"SELECT DISTINCT tc.table_name, kcu.column_name " +
				"FROM information_schema.table_constraints AS tc " + 
			    "JOIN information_schema.key_column_usage AS kcu " +
			    "ON tc.constraint_name = kcu.constraint_name " +
			    "AND tc.table_schema = kcu.table_schema " +
			    "JOIN information_schema.constraint_column_usage AS ccu " +
			    "ON ccu.constraint_name = tc.constraint_name " +
			    "AND ccu.table_schema = tc.table_schema " +
			    "WHERE (tc.constraint_type = 'FOREIGN KEY' " +
			    "OR tc.constraint_type = 'PRIMARY KEY') AND " +
			    "tc.table_name NOT LIKE '" + NamingConfig.SKINNER_PREFIX + "%';");
		while (result.next()) {
			columns.add(new ColumnRef(
					result.getString(1), result.getString(2)));
		}
		return columns;
	}
}
