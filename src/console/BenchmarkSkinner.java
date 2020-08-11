package console;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import config.NamingConfig;
import connector.PgConnector;
import execution.Master;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import statistics.GeneralStats;
import statistics.JoinStats;
import statistics.PostStats;
import statistics.PreStats;

/**
 * Executes a benchmark on queries in a given directory.
 * Writes out query result tuples and execution times
 * into two files in that same directory.
 * 
 * @author immanueltrummer
 *
 */
public class BenchmarkSkinner {
	/**
	 * Load queries, contained in .sql files in specified directory,
	 * in alphabetical order of file names.
	 * 
	 * @param queryDirPath	directory containing queries
	 * @return				sorted mapping from query names to queries
	 * @throws Exception
	 */
	static Map<String, PlainSelect> readQueries(String queryDirPath) 
			throws Exception {
		Map<String, PlainSelect> queries = 
				new TreeMap<String, PlainSelect>();
		File queryDir = new File(queryDirPath);
		String[] fileNames = queryDir.list();
		Arrays.sort(fileNames);
		for (String fileName : fileNames) {
			if (fileName.endsWith(".sql")) {
				String fullPath = queryDirPath + "/" + fileName;
				String sql = new String(Files.readAllBytes(
						Paths.get(fullPath)));
				Statement sqlStatement = CCJSqlParserUtil.parse(sql);
				PlainSelect plainSelect = (PlainSelect)((Select)
						sqlStatement).getSelectBody();
				queries.put(fileName, plainSelect);
			}
		}
		return queries;
	}
	/**
	 * Write query result to print stream.
	 * 
	 * @param result	query result set
	 * @param out		print stream
	 * @throws Exception
	 */
	static void printQueryResult(ResultSet result, PrintWriter out) throws Exception {
		int nrColumns = result.getMetaData().getColumnCount();
		while (result.next()) {
			for (int colCtr=1; colCtr<nrColumns; ++colCtr) {
				out.print(result.getString(colCtr));
				out.print(", ");
				
			}
			out.println(result.getString(nrColumns));
		}
		out.flush();
	}
	/**
	 * Reads queries from files in alphabetical order, executes
	 * them and writes benchmark results to files.
	 * 
	 * @param args			need to specify query directory
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// Check whether command line arguments are given 
		if (args.length < 5) {
			System.out.println("Error - you must specify database name,"
					+ "user name, password, the path of a directory" + 
					" containing queries to benchmark, and system!");
			return;
		}
		// Load queries from file
		String database = args[0];
		String username = args[1];
		String password = args[2];
		String queryDirPath = args[3];
		String system = args[4];
		if (!(system.equals("skinner") || 
				system.equals("postgres"))) {
			System.out.println("Error - specify skinner "
					+ "or postgres as system!");
			return;
		}
		boolean benchSkinner = system.equals("skinner");
		Map<String, PlainSelect> queries = readQueries(queryDirPath);
		// Create files to output benchmark results
		String pathPrefix = queryDirPath + "/" + system;
		String resultFilePath = pathPrefix + "results.txt";
		String timeFilePath = pathPrefix + "times.txt";
		PrintWriter resultOut = new PrintWriter(resultFilePath);
		PrintWriter timeOut = new PrintWriter(timeFilePath);
		// Prepare database connection
		PgConnector.connect("jdbc:postgresql:" + 
				database, username, password);
		// Execute queries and write results to file
		for (Entry<String, PlainSelect> entry : queries.entrySet()) {
			String queryID = "genericx";
			System.out.println("Benchmarking " + entry.getKey());
			if (benchSkinner) {
				Master.execute(entry.getValue(), queryID);
				// Write out execution time
				timeOut.println(entry.getKey() + "\t" +
						PreStats.lastMillis + "\t" + 
						JoinStats.lastMillis + "\t" + 
						PostStats.lastMillis + "\t" + 
						GeneralStats.lastExecutionTime + "\t" +
						GeneralStats.lastNonBatchedTime + "\t" +
						GeneralStats.lastUsedLearning);				
				// Output query result
				String finalResultTable = NamingConfig.FINAL_TBL + queryID;
				ResultSet SkinnerResult = PgConnector.query(
						"SELECT * FROM " + finalResultTable + ";");
				resultOut.println(entry.getKey());
				printQueryResult(SkinnerResult, resultOut);
			} else {
				long startMillis = System.currentTimeMillis();
				//PgConnector.update("SET enable_nestloop = false");
				PgConnector.setTimeout(300000);
				resultOut.println(entry.getKey());
				try {
					ResultSet PostgresResult = PgConnector.query(
							entry.getValue().toString());
					printQueryResult(PostgresResult, resultOut);
				} catch (Exception e) {
					e.printStackTrace();
					resultOut.println("TIMEOUT");
				}
				long totalMillis = System.currentTimeMillis() - startMillis;
				timeOut.println(entry.getKey() + "\t" + totalMillis);
			}
			timeOut.flush();
			resultOut.flush();
		}
		// Close database connection
		PgConnector.deconnect();
		// Close result files
		resultOut.close();
		timeOut.close();
	}
}
