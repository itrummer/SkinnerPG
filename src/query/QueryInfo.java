package query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import catalog.ColumnInfo;
import catalog.PgCatalog;
import config.NamingConfig;
import expressions.ExpressionInfo;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.cnfexpression.CNFConverter;
import org.apache.commons.lang3.StringUtils;

/**
 * Contains information on the query to execute.
 * 
 * @author immanueltrummer
 *
 */
public class QueryInfo {
	/**
	 * Plain select statement query to execute.
	 */
	public final PlainSelect plainSelect;
	/**
	 * Number of table instances in FROM clause.
	 */
	public int nrJoined = 0;
	/**
	 * All aliases in the query's FROM clause.
	 */
	public String[] aliases;
	/**
	 * Maps each alias to its alias index.
	 */
	public Map<String, Integer> aliasToIndex =
			new HashMap<String, Integer>();
	/**
	 * Maps aliases to associated table name.
	 */
	public Map<String, String> aliasToTable = 
			new HashMap<String, String>();
	/**
	 * Maps unique column names to associated table aliases.
	 */
	public Map<String, String> columnToAlias =
			new HashMap<String, String>();
	/**
	 * Maps from aliases to SQL expressions.
	 */
	public Map<String, Expression> aliasToExpression =
			new HashMap<String, Expression>();
	/**
	 * Maps column reference to column info.
	 */
	public Map<ColumnRef, ColumnInfo> colRefToInfo =
			new HashMap<ColumnRef, ColumnInfo>();
	/**
	 * Expressions that appear in the SELECT clause
	 * with associated meta-data.
	 */
	public List<ExpressionInfo> selectExpressions =
			new ArrayList<ExpressionInfo>();
	/**
	 * Stores information on predicates in WHERE clause.
	 * Each expression integrates all predicates that
	 * refer to the same table instances.
	 */
	public List<ExpressionInfo> wherePredicates = 
			new ArrayList<ExpressionInfo>();
	/**
	 * List of expressions that correspond to unary
	 * predicates.
	 */
	public List<ExpressionInfo> unaryPredicates =
			new ArrayList<ExpressionInfo>();
	/**
	 * List of expressions that correspond to join
	 * predicates.
	 */	
	public List<ExpressionInfo> joinPredicates =
			new ArrayList<ExpressionInfo>();
	/**
	 * Sets of alias indices that are connected via
	 * join predicates - used to quickly determined
	 * eligible tables for continuing join orders
	 * while avoiding Cartesian product joins.
	 */
	public List<Set<Integer>> joinedIndices =
			new ArrayList<Set<Integer>>();
	/**
	 * Columns that are involved in binary equi-join
	 * predicates (i.e., we may want to create hash
	 * indices for them during pre-processing).
	 */
	public Set<ColumnRef> equiJoinCols =
			new HashSet<ColumnRef>();
	/**
	 * Expressions that appear in GROUP-BY clause with
	 * associated meta-data.
	 */
	public List<ExpressionInfo> groupByExpressions =
			new ArrayList<ExpressionInfo>();
	/**
	 * Expressions that appear in ORDER-BY clause with
	 * associated meta-data.
	 */
	public List<ExpressionInfo> orderByExpressions =
			new ArrayList<ExpressionInfo>();
	/**
	 * HAVING clause expression with meta-data.
	 */
	public ExpressionInfo havingExpression;
	/**
	 * Set of columns required for join processing.
	 */
	public Set<ColumnRef> colsForJoins =
			new HashSet<ColumnRef>();
	/**
	 * Set of columns required for deduplicating
	 * join result tuples.
	 */
	public Set<ColumnRef> colsForDedup =
			new HashSet<ColumnRef>();
	/**
	 * Set of columns required for post-processing.
	 */
	public Set<ColumnRef> colsForPostProcessing =
			new HashSet<ColumnRef>();
	/**
	 * Extract information from the FROM clause (e.g.,
	 * all tables referenced with their aliases, the
	 * number of items in the from clause etc.).
	 */
	void extractFromInfo() throws Exception {
		// Extract all from items
		List<FromItem> fromItems = new ArrayList<FromItem>();
		fromItems.add(plainSelect.getFromItem());
		for (Join join : plainSelect.getJoins()) {
			fromItems.add(join.getRightItem());
		}
		nrJoined = fromItems.size();
		// Extract tables from items
		aliases = new String[nrJoined];
		for (int i=0; i<nrJoined; ++i) {
			FromItem fromItem = fromItems.get(i);
			// Retrieve information on associated table
			Table table = (Table)fromItem;
			String alias = table.getAlias()!=null?
					table.getAlias().getName():
						table.getName();
			String tableName = table.getName();
			// Register mapping from alias to table
			aliasToTable.put(alias, tableName);
			// Register mapping from index to alias
			aliases[i] = alias;
			// Register mapping from alias to index
			aliasToIndex.put(alias, i);
			// Extract columns with types
			List<ColumnInfo> colsInfo = PgCatalog.columnMeta(tableName);
			for (ColumnInfo colInfo : colsInfo) {
				String colName = colInfo.columnName;
				colRefToInfo.put(new ColumnRef(alias, colName), colInfo);
			}
		}
	}
	/**
	 * Adds implicit table references via unique column names.
	 */
	void addImplicitRefs() throws Exception {
		for (Entry<String, String> entry : aliasToTable.entrySet()) {
			String alias = entry.getKey();
			String table = entry.getValue();
			List<ColumnInfo> columnsInfo = PgCatalog.columnMeta(table);
			for (ColumnInfo columnInfo : columnsInfo) {
				String columnName = columnInfo.tableName;
				if (columnToAlias.containsKey(columnName)) {
					columnToAlias.put(columnName, null);
				} else {
					columnToAlias.put(columnName, alias);
				}
			}
		}
	}
	/**
	 * Add expression aliases from the SELECT clause.
	 */
	void addSelectAliases() {
		// Iterate over items in select clause.
		for (SelectItem selectItem : 
			plainSelect.getSelectItems()) {
			if (selectItem instanceof SelectExpressionItem) {
				SelectExpressionItem exprItem = (SelectExpressionItem)selectItem;
				Expression expr = exprItem.getExpression();
				// Extract alias if any
				String alias = (exprItem.getAlias() != null?
						exprItem.getAlias().getName():null);
				ExpressionInfo exprInfo = new ExpressionInfo(
						this, expr, alias);
				selectExpressions.add(exprInfo);
				if (alias != null) {
					aliasToExpression.put(alias, 
							exprInfo.finalExpression);					
				}
			}
		}
	}
	/**
	 * Extracts all conjuncts from a nested AND expression
	 * via recursive calls. The result will be stored in
	 * the second parameter.
	 * 
	 * @param condition	the remaining condition (no conjuncts extracted yet)
	 * @param conjuncts	stores the resulting conjuncts
	 */
	static void extractConjuncts(Expression condition, 
			List<Expression> conjuncts) {
		if (condition instanceof AndExpression) {
			AndExpression and = (AndExpression)condition;
			extractConjuncts(and.getLeftExpression(), conjuncts);
			extractConjuncts(and.getRightExpression(), conjuncts);
		} else {
			conjuncts.add(condition);
		}
	}
	/**
	 * Extract columns that are involved in equi-joins
	 * from given expression.
	 * 
	 * @param exprInfo	potential equi-join predicate
	 */
	void extractEquiJoinCols(ExpressionInfo exprInfo) {
		Expression expr = exprInfo.finalExpression;
		if (expr instanceof EqualsTo) {
			EqualsTo equalsExpr = (EqualsTo)expr;
			Expression left = equalsExpr.getLeftExpression();
			Expression right = equalsExpr.getRightExpression();
			if (left instanceof Column && right instanceof Column) {
				Column leftCol = (Column)left;
				Column rightCol = (Column)right;
				ColumnRef leftRef = new ColumnRef(
						leftCol.getTable().getName(),
						leftCol.getColumnName());
				ColumnRef rightRef = new ColumnRef(
						rightCol.getTable().getName(),
						rightCol.getColumnName());
				equiJoinCols.add(leftRef);
				equiJoinCols.add(rightRef);
			}
		}
	}
	/**
	 * Extracts predicates from normalized WHERE clause, separating
	 * predicates by the tables they refer to.
	 */
	void extractPredicates() {
		Expression where = plainSelect.getWhere();
		if (where != null) {
			// Normalize WHERE clause and transform into CNF
			ExpressionInfo whereInfo = new ExpressionInfo(
					this, where, null);
			Expression normalizedWhere = whereInfo.finalExpression;
			// TODO: Debug CNF transformation
			//Expression cnfWhere = CNFConverter.convertToCNF(normalizedWhere);
			Expression cnfWhere = normalizedWhere;
			// Extract conjuncts from WHERE expression
			List<Expression> conjuncts = new ArrayList<Expression>();
			extractConjuncts(cnfWhere, conjuncts);
			//System.out.println(conjuncts);
			// Merge conditions that refer to the same tables
			Map<Set<String>, Expression> tablesToCondition = 
					new HashMap<Set<String>, Expression>();
			for (Expression conjunct : conjuncts) {
				ExpressionInfo conjunctInfo = new ExpressionInfo(
						this, conjunct, null);
				Set<String> tables = conjunctInfo.aliasesMentioned;
				if (tablesToCondition.containsKey(tables)) {
					Expression prior = tablesToCondition.get(tables);
					Expression curAndPrior = new AndExpression(prior, conjunct);
					tablesToCondition.put(tables, curAndPrior);
				} else {
					tablesToCondition.put(tables, conjunct);
				}
			}
			// Create predicates from merged expressions
			for (Expression condition : tablesToCondition.values()) {
				ExpressionInfo pred = new ExpressionInfo(
						this, condition, null);
				wherePredicates.add(pred);
			}
			// Separate into unary and join predicates
			for (ExpressionInfo exprInfo : wherePredicates) {
				if (exprInfo.aliasesMentioned.size() == 1) {
					unaryPredicates.add(exprInfo);
				} else {
					joinPredicates.add(exprInfo);
					// Calculate mentioned alias indexes
					Set<Integer> aliasIdxs = new HashSet<Integer>();
					for (String alias : exprInfo.aliasesMentioned) {
						aliasIdxs.add(aliasToIndex.get(alias));
					}
					joinedIndices.add(aliasIdxs);
					// Extract columns for equi-joins
					extractEquiJoinCols(exprInfo);
				}
			}
		}
	}
	/**
	 * Adds expressions in the GROUP-By clause (if any).
	 */
	void treatGroupBy() {
		if (plainSelect.getGroupByColumnReferences() != null) {
			for (Expression groupExpr : 
				plainSelect.getGroupByColumnReferences()) {
				groupByExpressions.add(new ExpressionInfo(
						this, groupExpr, null));
			}
		}
	}
	/**
	 * Adds expression in HAVING clause.
	 */
	void treatHaving() {
		Expression having = plainSelect.getHaving();
		if (having != null) {
			havingExpression = new ExpressionInfo(
					this, having, null);
		} else {
			havingExpression = null;
		}
	}
	/**
	 * Adds expression in ORDER BY clause.
	 */
	void treatOrderBy() {
		if (plainSelect.getOrderByElements() != null) {
			for (OrderByElement orderElement : 
				plainSelect.getOrderByElements()) {
				Expression expr = orderElement.getExpression();
				ExpressionInfo exprInfo = new ExpressionInfo(
						this, expr, null);
				orderByExpressions.add(exprInfo);
			}
		}
	}
	/**
	 * Collects columns required for steps after pre-processing.
	 */
	void collectRequiredCols() {
		colsForJoins.addAll(extractCols(joinPredicates));
		for (int aliasCtr=0; aliasCtr<nrJoined; ++aliasCtr) {
			colsForDedup.add(new ColumnRef(aliases[aliasCtr],
					NamingConfig.BATCH_ID_COLUMN));
		}
		colsForPostProcessing.addAll(extractCols(selectExpressions));
		colsForPostProcessing.addAll(extractCols(groupByExpressions));
		if (havingExpression != null) {
			colsForPostProcessing.addAll(havingExpression.columnsMentioned);			
		}
		colsForPostProcessing.addAll(extractCols(orderByExpressions));
	}
	/**
	 * Extracts a list of all columns mentioned in a list of
	 * expressions.
	 * 
	 * @param expressions	list of expressions to extract columns from
	 * @return				set of references to mentioned columns
	 */
	static Set<ColumnRef> extractCols(List<ExpressionInfo> expressions) {
		Set<ColumnRef> colRefs = new HashSet<ColumnRef>();
		for (ExpressionInfo expr : expressions) {
			colRefs.addAll(expr.columnsMentioned);
		}
		return colRefs;
	}
	/**
	 * Returns true if there it at least one join predicate
	 * connecting the set of items in the FROM clause to the
	 * single item. We assume that the new table is not in
	 * the set of already joined tables.
	 * 
	 * @param aliasIndices	indexes of aliases already joined
	 * @param newIndex		index of new alias to check
	 * @return				true iff join predicates connect
	 */
	public boolean connected(Set<Integer> aliasIndices, int newIndex) {
		// Resulting join indices if selecting new table for join
		Set<Integer> indicesAfterJoin = new HashSet<Integer>();
		indicesAfterJoin.addAll(aliasIndices);
		indicesAfterJoin.add(newIndex);
		// Is there at least one connecting join predicate?
		for (Set<Integer> joined : joinedIndices) {
			if (indicesAfterJoin.containsAll(joined) &&
					joined.contains(newIndex)) {
				return true;
			}
		}
		return false;
	}
	/**
	 * Concatenates string representations of given expression
	 * list, using the given separator.
	 * 
	 * @param expressions	list of expressions to concatenate
	 * @param separator		separator to insert between elements
	 * @return				concatenation string
	 */
	String concatenateExprs(List<ExpressionInfo> expressions, String separator) {
		List<String> toConcat = new ArrayList<String>();
		for (ExpressionInfo expr : expressions) {
			toConcat.add(expr.toString());
		}
		return StringUtils.join(toConcat, separator);
	}
	/**
	 * Generates SQL query with FROM items reordered
	 * according to given join order.
	 * 
	 * @param order	join order to adopt
	 * @return		SQL query with reordered FROM items
	 */
	public String reorderedQuery(int[] order) {
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("SELECT ");
		List<String> selectors = new ArrayList<String>();
		for (ExpressionInfo selector : selectExpressions) {
			selectors.add(selector.toString() + " AS " + selector.alias);
		}
		sqlBuilder.append(StringUtils.join(selectors, ", "));
		sqlBuilder.append(" FROM ");
		List<String> fromItems = new ArrayList<String>();
		for (int joinCtr=0; joinCtr<nrJoined; ++joinCtr) {
			int tableIdx = order[joinCtr];
			String alias = aliases[tableIdx];
			String table = aliasToTable.get(alias);
			fromItems.add(table + " AS " + alias);
		}
		sqlBuilder.append(StringUtils.join(fromItems, " CROSS JOIN "));
		if (!wherePredicates.isEmpty()) {
			sqlBuilder.append(" WHERE ");
			sqlBuilder.append(concatenateExprs(wherePredicates, " AND "));
		}
		if (!groupByExpressions.isEmpty()) {
			sqlBuilder.append(" GROUP BY ");
			sqlBuilder.append(concatenateExprs(groupByExpressions, ", "));
		}
		if (havingExpression != null) {
			sqlBuilder.append(" HAVING ");
			sqlBuilder.append(havingExpression.toString());
		}
		if (!orderByExpressions.isEmpty()) {
			sqlBuilder.append(" ORDER BY ");
			sqlBuilder.append(concatenateExprs(orderByExpressions, ", "));
		}
		return sqlBuilder.toString();
	}
	/**
	 * Analyzes a select query to prepare processing.
	 * 
	 * @param plainSelect	a plain select query
	 */
	public QueryInfo(PlainSelect plainSelect) throws Exception {
		this.plainSelect = plainSelect;
		// Extract information in FROM clause
		extractFromInfo();
		System.out.println("Alias -> table: " + aliasToTable);
		System.out.println("Column info: " + colRefToInfo);
		// Add implicit references to aliases
		addImplicitRefs();
		System.out.println("Unique column name -> alias: " + columnToAlias);
		// Add aliases for expressions in SELECT clause
		addSelectAliases();
		System.out.println("Select expressions: " + selectExpressions);
		// Extract predicates in WHERE clause
		extractPredicates();
		System.out.println("Unary predicates: " + unaryPredicates);
		System.out.println("Join predicates: " + joinPredicates);
		System.out.println("Equi join cols: " + equiJoinCols);
		// Add expressions in GROUP BY clause
		treatGroupBy();
		System.out.println("GROUP BY expressions: " + groupByExpressions);
		// Add expression in HAVING clause
		treatHaving();
		System.out.println("HAVING clause: " + 
				(havingExpression!=null?havingExpression:"none"));
		// Adds expressions in ORDER BY clause
		treatOrderBy();
		System.out.println("ORDER BY expressions: " + orderByExpressions);
		// Collect required columns
		collectRequiredCols();
		System.out.println("Required cols for joins: " + 
				colsForJoins);
		System.out.println("Required for post-processing: " + 
				colsForPostProcessing);
	}
}
