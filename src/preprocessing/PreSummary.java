package preprocessing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import expressions.ExpressionInfo;

/**
 * Summarizes processing steps taken during pre-processing.
 * 
 * @author immanueltrummer
 *
 */
public class PreSummary {
	/**
	 * Names of temporary tables created during last invocation.
	 */
	public final List<String> tempTables = new ArrayList<String>();
	/**
	 * Maps each alias to the corresponding table after pre-processing.
	 */
	public final Map<String, String> aliasToTable = new HashMap<String, String>();
	/**
	 * Maps each alias in FROM clause to comma-separated list of non-ID columns
	 * (useful for formulating queries during join phase)
	 */
	public final Map<String, String> aliasToNonIDcols = new HashMap<String, String>();
	/**
	 * Maps each alias in FROM clause to comma-separated list of non-ID columns
	 * with associated column types (useful for creating intermediate data structures).
	 */
	public final Map<String, String> aliasToTypedNonIDcols = new HashMap<String, String>();
	/**
	 * Maps table aliases to unary predicates to consider after pre-processing (in join phase).
	 */
	public final Map<String, ExpressionInfo> aliasToUnaryTodo = new HashMap<String, ExpressionInfo>();
	/**
	 * Maps table aliases to sets of non-empty batch IDs to process.
	 */
	public final Map<String, List<Integer>> aliasToTodoBatches = new HashMap<String, List<Integer>>();
	@Override
	public String toString() {
		return "Temp tables:\t" + tempTables.toString() + System.lineSeparator() +
				"Alias to tables:\t" + aliasToTable.toString() + System.lineSeparator() +
				"Alias to non-ID cols:\t" + aliasToNonIDcols.toString() + System.lineSeparator() +
				"Alias to typed non-ID cols:\t" + aliasToTypedNonIDcols.toString() + System.lineSeparator() +
				"Alias to remaining unary predicates:\t" + aliasToUnaryTodo.toString() + System.lineSeparator() +
				"Determined non-empty batches for:\t" + aliasToTodoBatches.keySet().toString();
	}
}
