package joining;

import java.util.List;

/**
 * A summary of processing steps taken
 * during the join phase.
 * 
 * @author immanueltrummer
 *
 */
public class JoinSummary {
	/**
	 * Name of the table storing join result.
	 */
	public final String resultTable;
	/**
	 * Names of columns in join result table.
	 */
	public final List<String> resultTableColumns;
	/**
	 * Whether post-processing was finished as
	 * part of fast-forwarding.
	 */
	public final boolean finishedPostProceccing;
	/**
	 * Initializes join phase summary.
	 * 
	 * @param resultTable				name of temporary table in
	 * 									which join phase result is
	 * 									stored.
	 * @param resultTableColumns		List of join result table columns.
	 * @param finishedPostProcessing	whether post-processing was finished already.
	 */
	public JoinSummary(String resultTable, List<String> resultTableColumns,
			boolean finishedPostProcessing) {
		this.resultTable = resultTable;
		this.resultTableColumns = resultTableColumns;
		this.finishedPostProceccing = finishedPostProcessing;
	}
}
