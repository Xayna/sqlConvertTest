package demo.metamodel;

import java.util.stream.Collectors;

import demo.metamodel.IModel.ICKConstraint;
import demo.metamodel.IModel.IColumn;
import demo.metamodel.IModel.ISchema;
import demo.metamodel.IModel.ITable;
import demo.util.DataType;
import demo.util.MapExpression;

public class CKConstraint extends Constraint implements ICKConstraint {

	private String constraintClause;
	private String constraintColumn;
	private String colType;

	public CKConstraint(String consSchema, String consName, String consType,
			ISchema tableSchema, ITable table, boolean deferable,
			boolean initiallyDefered) {
		super(consSchema, consName, consType, tableSchema, table, deferable,
				initiallyDefered);
	}

	@Override
	public String getConstraintColumn() {
		return constraintColumn;
	}

	@Override
	public void setConstraintColumn(String columnName) {
		constraintColumn = columnName;
	
		colType = getTable()
				.getColumns()
				.stream()
				.filter(col -> col.toString().equalsIgnoreCase(
						columnName)).map(col -> col.getDataType())
				.collect(Collectors.joining());
		//System.out.println( getTable()
			//	.getColumns().size() + " : col type : " + colType);

	}

	@Override
	public String getConstraintClause() {
		return constraintClause;
	}

	@Override
	public void setConstraintClause(String clause) {

		String temp = clause.replace("'[", "'{").replace("]'", "}'");
		temp = temp.replace("[", "").replace("]", "");
		temp = temp.replace("{", "[").replace("}", "]");
		temp = temp.substring(temp.indexOf('(') + 1, temp.lastIndexOf(')'));
		temp = MapExpression.getFormatedExpression(temp, colType);

		//System.out.println(temp);
		this.constraintClause = temp;
	}

	@Override
	public String toSQL() {
		String sql = "ALTER TABLE " + getTableSchema() + "."
				+ getTable() + " ADD ";
		sql += "CONSTRAINT " + toString() + " ";
		sql += "CHECK (" + getConstraintClause() + ")";

		return sql;

	}

}
