package config;

/**
 * Configures how SkinnerDB names tables and columns
 * it creates during processing.
 * 
 * @author immanueltrummer
 *
 */
public class NamingConfig {
	/**
	 * Common prefix for all elements created by SkinnerDB.
	 */
	public final static String SKINNER_PREFIX = "skinner";
	/**
	 * Common prefix for all Skinner index names.
	 */
	public final static String SKINNER_INDEX = SKINNER_PREFIX + "index";
	/**
	 * Prefix added to base tables clustered by batch ID.
	 */
	public final static String CLUSTER_TBL = SKINNER_PREFIX + "clustered";
	/**
	 * Prefix added to base tables after filtering.
	 */
	public final static String FILTER_TBL = SKINNER_PREFIX + "filtered";
	/**
	 * Prefix added to table containing join result.
	 */
	public final static String JOIN_TBL = SKINNER_PREFIX + "joined";
	/**
	 * Prefix added to table storing final result after post-processing.
	 */
	public final static String FINAL_TBL = SKINNER_PREFIX + "result";
	/**
	 * Name of table column storing batch ID for each tuple.
	 */
	public final static String BATCH_ID_COLUMN = "SkinnerBatchID";
	/**
	 * Generates name of index for given table and column.
	 * Naming indices consistently across different stages is
	 * important since we use the "CREATE INDEX IF NOT EXISTS"
	 * command of Postgres to avoid redundant index creation
	 * (this command is based on index name).
	 * 
	 * @param table		name of table on which index is created
	 * @param column	name of column that is being indexed
	 * @return			name for index to create
	 */
	public static String indexName(String table, String column) {
		return SKINNER_INDEX + table + column;
	}
}
