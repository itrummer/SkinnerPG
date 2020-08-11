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
	 * Initializes post-processing result.
	 * 
	 * @param resultTable	name of table storing final result
	 */
	public PostSummary(String resultTable) {
		this.resultTable = resultTable;
	}
}
