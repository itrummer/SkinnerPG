package console;

import java.sql.ResultSet;

import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.impl.history.DefaultHistory;

import config.NamingConfig;
import connector.PgConnector;
import execution.Master;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

/**
 * Command line interface for running SkinnerDB on
 * top of Postgres. Uses reinforcement learning to
 * 
 * @author immanueltrummer
 *
 */
public class CLI {
	/**
	 * Write query result to print stream.
	 * 
	 * @param result	query result set
	 * @throws Exception
	 */
	static void printQueryResult(ResultSet result) throws Exception {
		int nrColumns = result.getMetaData().getColumnCount();
		while (result.next()) {
			for (int colCtr=1; colCtr<nrColumns; ++colCtr) {
				System.out.print(result.getString(colCtr));
				System.out.print(", ");
				
			}
			System.out.println(result.getString(nrColumns));
		}
		System.out.flush();
	}
	/**
	 * Process given input and check for termination.
	 * 
	 * @param input		input command
	 * @return			true if query processing terminated
	 * @throws Exception
	 */
	static boolean processInput(String input) throws Exception {
		// Check for termination
		if (input.equalsIgnoreCase("quit")) {
			return true;
		}
		// Try parsing query
		Statement sqlStatement = CCJSqlParserUtil.parse(input);
		PlainSelect plainSelect = (PlainSelect)((Select)
				sqlStatement).getSelectBody();
		// Process parsed query
		String queryID = "defaultQuery";
		Master.execute(plainSelect, queryID);
		// Output query result
		String finalResultTable = NamingConfig.FINAL_TBL + queryID;
		ResultSet SkinnerResult = PgConnector.query(
				"SELECT * FROM " + finalResultTable + ";");
		printQueryResult(SkinnerResult);
		// No termination
		return false;
	}
	/**
	 * Processes input queries on given Postgres
	 * database using reinforcement learning.
	 * Generates query result and optimized
	 * query rewriting.
	 * 
	 * @param args	database, user name, password (optional)	
	 */
	public static void main(String[] args) throws Exception {
		// Check whether command line arguments are given 
		if (args.length < 2) {
			System.out.println("Error - you must specify "
					+ "database name,user name, and "
					+ "(optionally) password!");
			return;
		}
		// Extract info from command line parameters 
		String database = args[0];
		String username = args[1];
		String password = args.length>=2?args[2]:"";
		// Prepare database connection
		PgConnector.connect("jdbc:postgresql:" + 
				database, username, password);
		// Start reading input from console
        LineReader reader = LineReaderBuilder.builder()
                .history(new DefaultHistory()).build();
        boolean terminated = false;
        while (!terminated) {
        	String input = reader.readLine("skinner> ");
        	try {
            	terminated = processInput(input); 
            } catch (Exception e) {
                System.err.println("Error processing command: ");
                e.printStackTrace();
            }
        }
	}
}
