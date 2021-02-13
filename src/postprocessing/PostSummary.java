package postprocessing;

/**
 * Summarizes processing steps taken during post-processing.
 * 
 * @author immanueltrummer
 *
 */
public class PostSummary {
	/**
	 * Contains name of table storing final query result.
	 */
	public final String resultTable;
	/**
	 * Whether post-processing was influenced by
	 * a timeout.
	 */
	public final boolean timeout;
	/**
	 * Initializes post-processing result.
	 * 
	 * @param resultTable	name of table storing final result
	 * @param timeout		timeout in post-processing?
	 */
	public PostSummary(String resultTable, boolean timeout) {
		this.resultTable = resultTable;
		this.timeout = timeout;
	}
}
