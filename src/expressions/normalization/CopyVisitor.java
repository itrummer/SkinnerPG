package expressions.normalization;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.KeepExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.MySQLGroupConcat;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.OracleHint;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.WithinGroupExpression;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.JsonOperator;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;

/**
 * Copies the given expression by creating new inner nodes.
 * This class serves as super class for all rewriting classes
 * which only substitute a subset of node types.
 * 
 * @author immanueltrummer
 *
 */
public class CopyVisitor implements ExpressionVisitor {
	/**
	 * Copied expression fragments - finally contains copied expression.
	 */
	public Deque<Expression> exprStack = new ArrayDeque<>();

	@Override
	public void visit(NullValue arg0) {
		exprStack.push(arg0);
	}

	@Override
	public void visit(Function arg0) {
		// Visit function parameter expressions
		List<Expression> params = arg0.getParameters().getExpressions();
		for (Expression param : params) {
			param.accept(this);
		}
		// Combine rewritten operands in expression list
		int nrParams = params.size();
		List<Expression> newParams = new ArrayList<Expression>();
		for (int i=0; i<nrParams; ++i) {
			newParams.add(0, exprStack.pop());
		}
		// Create new function expression and push on the stack
		Function newFunction = new Function();
		newFunction.setName(arg0.getName());
		newFunction.setDistinct(arg0.isDistinct());
		newFunction.setEscaped(arg0.isEscaped());
		newFunction.setKeep(arg0.getKeep());
		newFunction.setParameters(new ExpressionList(newParams));
		newFunction.setAllColumns(arg0.isAllColumns());
		exprStack.push(newFunction);
	}

	@Override
	public void visit(SignedExpression arg0) {
		arg0.getExpression().accept(this);
		exprStack.push(new SignedExpression(
				arg0.getSign(), exprStack.pop()));
	}

	@Override
	public void visit(JdbcParameter arg0) {
		exprStack.push(arg0);
	}

	@Override
	public void visit(JdbcNamedParameter arg0) {
		exprStack.push(arg0);
	}

	@Override
	public void visit(DoubleValue arg0) {
		exprStack.push(arg0);
	}

	@Override
	public void visit(LongValue arg0) {
		exprStack.push(arg0);
	}

	@Override
	public void visit(HexValue arg0) {
		exprStack.push(arg0);
	}

	@Override
	public void visit(DateValue arg0) {
		exprStack.push(arg0);
	}

	@Override
	public void visit(TimeValue arg0) {
		exprStack.push(arg0);
	}

	@Override
	public void visit(TimestampValue arg0) {
		exprStack.push(arg0);
	}

	@Override
	public void visit(Parenthesis arg0) {
		arg0.getExpression().accept(this);
		Parenthesis newParenthesis = new Parenthesis(exprStack.pop());
		exprStack.push(newParenthesis);
	}

	@Override
	public void visit(StringValue arg0) {
		exprStack.push(arg0);
	}
	/**
	 * Copies a binary expression.
	 * 
	 * @param oldBinaryOp	original binary operation expression
	 * @param newBinaryOp	new binary operation expression
	 */
	void treatBinary(BinaryExpression oldBinaryOp, 
			BinaryExpression newBinaryOp) {
		// Recursive invocation fills operand stack
		oldBinaryOp.getLeftExpression().accept(this);
		oldBinaryOp.getRightExpression().accept(this);
		// Obtain rewritten operands from stack
		Expression op2 = exprStack.pop();
		Expression op1 = exprStack.pop();
		// Create and push copy to stack
		newBinaryOp.setLeftExpression(op1);
		newBinaryOp.setRightExpression(op2);
		exprStack.push(newBinaryOp);
	}

	@Override
	public void visit(Addition arg0) {
		Addition newAddition = new Addition();
		treatBinary(arg0, newAddition);
	}

	@Override
	public void visit(Division arg0) {
		Division newDivision = new Division();
		treatBinary(arg0, newDivision);
	}

	@Override
	public void visit(Multiplication arg0) {
		Multiplication newMultiplication = new Multiplication();
		treatBinary(arg0, newMultiplication);
	}

	@Override
	public void visit(Subtraction arg0) {
		Subtraction newSubtraction = new Subtraction();
		treatBinary(arg0, newSubtraction);
	}

	@Override
	public void visit(AndExpression arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		Expression op2 = exprStack.pop();
		Expression op1 = exprStack.pop();
		exprStack.push(new AndExpression(op1, op2));
	}

	@Override
	public void visit(OrExpression arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		Expression op2 = exprStack.pop();
		Expression op1 = exprStack.pop();
		exprStack.push(new OrExpression(op1, op2));
	}

	@Override
	public void visit(Between arg0) {
		Between newBetween = new Between();
		arg0.getLeftExpression().accept(this);
		newBetween.setLeftExpression(exprStack.pop());
		arg0.getBetweenExpressionStart().accept(this);
		newBetween.setBetweenExpressionStart(exprStack.pop());
		arg0.getBetweenExpressionEnd().accept(this);
		newBetween.setBetweenExpressionEnd(exprStack.pop());
		exprStack.push(newBetween);
	}

	@Override
	public void visit(EqualsTo arg0) {
		EqualsTo newEquals = new EqualsTo();
		if (arg0.isNot()) {
			newEquals.setNot();
		}
		treatBinary(arg0, newEquals);
	}

	@Override
	public void visit(GreaterThan arg0) {
		GreaterThan newGt = new GreaterThan();
		treatBinary(arg0, newGt);
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		GreaterThanEquals newGte = new GreaterThanEquals();
		treatBinary(arg0, newGte);
	}
	/**
	 * We transform an in expression into nested OR expressions.
	 */
	@Override
	public void visit(InExpression arg0) {
		InExpression newIn = new InExpression();
		newIn.setNot(arg0.isNot());
		arg0.getLeftExpression().accept(this);
		newIn.setLeftExpression(exprStack.pop());
		ItemsList rightList = arg0.getRightItemsList();
		if (rightList instanceof ExpressionList) {
			ExpressionList rightExprList = (ExpressionList)rightList;
			List<Expression> newExpressions = new ArrayList<Expression>();
			for (Expression expr : rightExprList.getExpressions()) {
				expr.accept(this);
				newExpressions.add(0, exprStack.pop());
			}
			newIn.setRightItemsList(new ExpressionList(newExpressions));
			exprStack.push(newIn);
		}
		// TODO: Otherwise not supported
	}

	@Override
	public void visit(IsNullExpression arg0) {
		arg0.getLeftExpression().accept(this);
		Expression newLeft = exprStack.pop();
		if (newLeft instanceof NullValue) {
			exprStack.push(new NullValue());
		} else {
			IsNullExpression isNull = new IsNullExpression();
			isNull.setLeftExpression(newLeft);
			isNull.setNot(arg0.isNot());
			exprStack.push(isNull);
		}
	}

	@Override
	public void visit(LikeExpression arg0) {
		arg0.getLeftExpression().accept(this);
		Expression newLeft = exprStack.pop();
		arg0.getRightExpression().accept(this);
		Expression newRight = exprStack.pop();
		LikeExpression newLike = new LikeExpression();
		newLike.setLeftExpression(newLeft);
		newLike.setRightExpression(newRight);
		newLike.setNot(arg0.isNot());
		newLike.setCaseInsensitive(arg0.isCaseInsensitive());
		newLike.setEscape(arg0.getEscape());
		exprStack.push(newLike);
	}

	@Override
	public void visit(MinorThan arg0) {
		MinorThan newMt = new MinorThan();
		treatBinary(arg0, newMt);
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		MinorThanEquals newMte = new MinorThanEquals();
		treatBinary(arg0, newMte);
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		NotEqualsTo newNe = new NotEqualsTo();
		treatBinary(arg0, newNe);
	}

	@Override
	public void visit(Column arg0) {
		exprStack.push(arg0);
	}

	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CaseExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WhenClause arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ExistsExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Concat arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		Expression op2 = exprStack.pop();
		Expression op1 = exprStack.pop();
		Concat newConcat = new Concat();
		newConcat.setLeftExpression(op1);
		newConcat.setRightExpression(op2);
		exprStack.push(newConcat);			
	}

	@Override
	public void visit(Matches arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseAnd arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseOr arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseXor arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CastExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Modulo arg0) {
		Modulo newModulo = new Modulo();
		treatBinary(arg0, newModulo);
	}

	@Override
	public void visit(AnalyticExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WithinGroupExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ExtractExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(IntervalExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(OracleHierarchicalExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(RegExpMatchOperator arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JsonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JsonOperator arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(RegExpMySQLOperator arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(UserVariable arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(NumericBind arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(KeepExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(MySQLGroupConcat arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(RowConstructor arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(OracleHint arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimeKeyExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DateTimeLiteralExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(NotExpression arg0) {
		arg0.getExpression().accept(this);
		Expression newExp = exprStack.pop();
		exprStack.push(new NotExpression(newExp));
	}
}
