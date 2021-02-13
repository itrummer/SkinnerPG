package prepare;

import java.util.List;

import catalog.PgCatalog;
import config.JoinConfig;
import config.NamingConfig;
import connector.PgConnector;
import query.ColumnRef;

/**
 * Prepares the database for more efficient
 * intra-query learning.
 * 
 * @author immanueltrummer
 *
 */
public class Preparator {
	/**
	 * Given database name, user name, and password, iterates
	 * over all base relations in database and creates indexed
	 * and clustered versions of them. This speeds up query
	 * processing at run time.
	 * 
	 * @param args	database name, user name, and database password
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// Check for command line arguments
		if (args.length < 2) {
			System.out.println("Error - specify database, user name, ");
			System.out.println("and (optionally) database password. ");
			return;
		}
		// Create connection based on command line parameters
		String dbName = args[0];
		String userName = args[1];
		String password = args.length<3?"":args[2];
		PgConnector.connect("jdbc:postgresql:" + dbName, 
				userName, password);
		// Get list of tables to prepare
		List<String> tables = PgCatalog.baseTables();
		// Iterate over tables
		for (String table : tables) {
			System.out.println("Treating table " + table + " ...");
			String clusteredTable = NamingConfig.CLUSTER_TBL + table;
			// Sort table by batch ID
			clusterTable(table, clusteredTable);
		}
		// Get list of constrained columns
		List<ColumnRef> columns = PgCatalog.constrainedColumns();
		// Iterate over constrained columns
		for (ColumnRef colRef : columns) {
			createIndex(colRef);
		}
	}
	/**
	 * Based on source table, create a new clustered table that contains
	 * one new column containing the batch ID and whose tuples are sorted
	 * by that batch ID. 
	 * 
	 * @param table				source table
	 * @param clusteredTable	clustered table to create
	 * @throws Exception
	 */
	static void clusterTable(String table, String clusteredTable) throws Exception {
		System.out.println("Clustering table " + table + " ...");
		// Clean up if necessary
		PgConnector.dropTable(clusteredTable);
		// Create new clustered table
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("CREATE TABLE ");
		sqlBuilder.append(clusteredTable);
		sqlBuilder.append(" AS (SELECT *, ");
		sqlBuilder.append("(id * 19 + 23) % ");
		sqlBuilder.append(JoinConfig.nrBatches);
		sqlBuilder.append(" AS ");
		sqlBuilder.append(NamingConfig.BATCH_ID_COLUMN);
		sqlBuilder.append(" FROM ");
		sqlBuilder.append(table);
		sqlBuilder.append(" ORDER BY ");
		sqlBuilder.append(NamingConfig.BATCH_ID_COLUMN);
		sqlBuilder.append(");");
		PgConnector.update(sqlBuilder.toString());
		// Create index for fast sorting
		System.out.println("Creating batch ID index for " + clusteredTable + " ...");
		sqlBuilder = new StringBuilder();
		sqlBuilder.append("CREATE INDEX IF NOT EXISTS ");
		sqlBuilder.append(NamingConfig.indexName(
				clusteredTable, NamingConfig.BATCH_ID_COLUMN));
		sqlBuilder.append(" ON ");
		sqlBuilder.append(clusteredTable);
		sqlBuilder.append(" (");
		sqlBuilder.append(NamingConfig.BATCH_ID_COLUMN);
		sqlBuilder.append(");");
		PgConnector.update(sqlBuilder.toString());
	}
	/**
	 * Create an index on given column.
	 * 
	 * @param colRef	reference to column to index
	 * @throws Exception
	 */
	static void createIndex(ColumnRef colRef) throws Exception {
		String table = colRef.aliasName;
		String clusteredTable = NamingConfig.CLUSTER_TBL + table;
		String column = colRef.columnName;
		System.out.println("Creating index for " + 
				clusteredTable + " on " + column + " ...");
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("CREATE INDEX IF NOT EXISTS ");
		sqlBuilder.append(NamingConfig.indexName(clusteredTable, column));
		sqlBuilder.append(" ON ");
		sqlBuilder.append(clusteredTable);
		sqlBuilder.append(" (");
		sqlBuilder.append(column);
		sqlBuilder.append(");");
		PgConnector.update(sqlBuilder.toString());
	}
}
