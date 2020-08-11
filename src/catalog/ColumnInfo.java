package catalog;

/**
 * Contains meta-data about a table column.
 * 
 * @author immanueltrummer
 *
 */
public class ColumnInfo {
	/**
	 * Name of table the column belongs to.
	 */
	public final String tableName;
	/**
	 * Name of table column.
	 */
	public final String columnName;
	/**
	 * SQL data type of column.
	 */
	public final String columnType;
	/**
	 * Stores table, column name and type.
	 * 
	 * @param tableName		table name
	 * @param columnName	column name
	 * @param columnType	type of column
	 */
	public ColumnInfo(String tableName, String columnName, String columnType) {
		this.tableName = tableName;
		this.columnName = columnName;
		this.columnType = columnType;
	}
	@Override
	public String toString() {
		return tableName + "." + columnName + " : " + columnType;
	}
}
