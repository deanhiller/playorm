package com.alvazan.orm.parser.antlr;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import javax.inject.Inject;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;
import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnToManyMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnToOneMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.TypeInfo;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;

public class ScannerSql {

	private static final Logger log = LoggerFactory.getLogger(ScannerSql.class);
	
	@Inject
	private Optimizer optimizer;
	
	public ExpressionNode compileSql(String query, InfoForWiring wiring, MetaFacade facade) {
		CommonTree theTree = parseTree(query);
		walkTheTree(theTree, wiring, facade);
		validateNoPartitionsMissed(theTree, wiring);
		ExpressionNode node = wiring.getAstTree();
		
		facade.setAttributeUserCount(wiring.getAttributeUsedCount());
		ExpressionNode newTree = (ExpressionNode) optimizer.optimize(node, facade, query);
		
		return newTree;
	}
	
	private void validateNoPartitionsMissed(CommonTree theTree, InfoForWiring wiring) {
		ViewInfoImpl noAliasTable = wiring.getNoAliasTable();
		if(noAliasTable != null) {
			if(noAliasTable.getTableMeta().getPartitionedColumns().size() > 0)
				throw new IllegalArgumentException("In your from you have a table defined with no alias" +
						"(ie. no 'as p' part) and your table is partitioned so you " +
						"need to alias and then define PARTITIONS p(:partitionid) SELECT etc. etc.");
		}		
		
		for(ViewInfo view : wiring.getAllViews()) {
			ViewInfoImpl t = (ViewInfoImpl) view;
			DboTableMeta meta = t.getTableMeta();
			if(meta.getPartitionedColumns().size() > 0 && t.getPartition() == null)
				throw new IllegalArgumentException("You are missing a definition of a partition for alias='"+view.getAlias()+"' since table="
			+meta.getColumnFamily()+" IS a partitioned table.  For example, you should have PARTITIONS e(:partitionId) SELECT e FROM TABLE as e....");
		}
	}

	private static CommonTree parseTree(String query) {
        ANTLRStringStream stream = new ANTLRStringStream(query);
        NoSqlLexer lexer = new NoSqlLexer(stream);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        NoSqlParser parser = new NoSqlParser(tokenStream);

        try {
        	return (CommonTree) parser.statement().getTree();
        } catch (RecognitionException re) {
        	ParseQueryException e = new ParseQueryException(" (Check the token after the one the parse complained about.  Are you missing the 'as' keyword", re);
        	throw e;
        }
    }
	
	@SuppressWarnings("unchecked")
	private <T> void walkTheTree(CommonTree tree, InfoForWiring wiring, MetaFacade facade) {
		int type = tree.getType();
		switch (type) {
		//The tree should come in with this order OR we are in BIG TROUBLE as we need the information
		//in the from clause first to get alias and what table the alias maps too
		case NoSqlLexer.FROM_CLAUSE:
			compileFromClause(tree, wiring, facade);
			break;
		case NoSqlLexer.JOIN_CLAUSE:
			compileJoinClause(tree, wiring, facade);
			break;
		case NoSqlLexer.PARTITIONS_CLAUSE:
			compilePartitionsClause(tree, wiring, facade);
			break;
		case NoSqlLexer.SELECT_CLAUSE:
			compileSelectClause(tree, wiring);
			break;
		case NoSqlLexer.WHERE:
			//We should try to get rid of the where token in the grammar so we don't need 
			//this line of code here....
			CommonTree expression = (CommonTree)tree.getChildren().get(0);
			ExpressionNode node = new ExpressionNode(expression);
			compileExpression(node, wiring, facade);
			wiring.setAstTree(node);
			break;

		case 0: // nil
			List<CommonTree> childrenList = tree.getChildren();
			for (CommonTree child : childrenList) {
				walkTheTree(child, wiring, facade);
			}
			break;
		default:
			break;
		}
	}

	@SuppressWarnings("unchecked")
	private <T> void compileJoinClause(CommonTree tree,
			InfoForWiring wiring, MetaFacade facade) {
		List<CommonTree> children = tree.getChildren();
		
		for(CommonTree child : children) {
			int type = child.getType();
			if(type == NoSqlLexer.LEFT_OUTER_JOIN) {
				compileJoin(child, wiring, facade, JoinType.LEFT_OUTER);
			} else if(type == NoSqlLexer.INNER_JOIN) {
				compileJoin(child, wiring, facade, JoinType.INNER);
			} else
				throw new UnsupportedOperationException("bug?, type="+type+" and we don't process that type for joins");
		}
	}
	
	@SuppressWarnings("unchecked")
	private <T> void compileJoin(CommonTree tree, InfoForWiring wiring, MetaFacade facade, JoinType type) {
		List<CommonTree> children = tree.getChildren();
		CommonTree aliasedColumn = children.get(0);
		CommonTree aliasNode = (CommonTree) aliasedColumn.getChild(0);
		CommonTree newAliasNode = children.get(1);
		String column = aliasedColumn.getText();
		String alias = aliasNode.getText();
		String newAlias = newAliasNode.getText();
		
		ViewInfoImpl tableInfo = wiring.getInfoFromAlias(alias);
		DboTableMeta tableMeta = tableInfo.getTableMeta();
		DboColumnMeta columnMeta = facade.getFkMetaIfExist(tableMeta, column);
		if(!(columnMeta instanceof DboColumnToOneMeta))
			throw new IllegalArgumentException("Column="+column+" on table="+tableMeta.getColumnFamily()+" is NOT a OneToOne NOR ManyToOne relationship according to our meta data");
		else if(!columnMeta.isIndexed())
			throw new IllegalArgumentException("Column="+column+" on table="+tableMeta.getColumnFamily()+" is not indexed.  Add @NoSqlIndex or map/reduce a new index in place and add annotation");
		DboColumnToOneMeta toOne = (DboColumnToOneMeta) columnMeta;
		DboTableMeta fkTableMeta = toOne.getFkToColumnFamily();

		ViewInfoImpl existing = wiring.getInfoFromAlias(newAlias);
		if(existing == null) {
			existing = new ViewInfoImpl(newAlias, fkTableMeta);
			wiring.putAliasTable(newAlias, existing);
		}
		
		//since this is an inner join on primary key, use id column
		DboColumnIdMeta colMeta2 = existing.getTableMeta().getIdColumnMeta();
		JoinInfo join = new JoinInfo(tableInfo, columnMeta, existing, colMeta2, type);
		
		tableInfo.addJoin(join);
		existing.addJoin(join);
	}
	
	@SuppressWarnings("unchecked")
	private <T> void compilePartitionsClause(CommonTree tree,
			InfoForWiring wiring, MetaFacade facade) {
		List<CommonTree> childrenList = tree.getChildren();
		for(CommonTree child : childrenList) {
			compileSinglePartition(wiring, child, facade);
		}
	}

	private void compileSinglePartition(InfoForWiring wiring, CommonTree child, MetaFacade facade) {
		String alias = child.getText();
		ViewInfoImpl info = wiring.getInfoFromAlias(alias);
		if(info == null)
			throw new IllegalArgumentException("In your PARTITIONS clause, you have an alias='"+alias+"' that is not found in your FROM clause");
		else if(info.getPartition() != null)
			throw new IllegalArgumentException("In your PARTITIONS clause, you define a partition for alias='"+alias+"' twice.  you can only define it once");
		
		DboColumnMeta partitionColumn;
		DboTableMeta tableMeta = info.getTableMeta();
		List<DboColumnMeta> partitionedColumns = tableMeta.getPartitionedColumns();
		if(partitionedColumns.size() == 0)
			throw new IllegalArgumentException("The meta data contains no partitions for table="+tableMeta.getColumnFamily()+" so your alias="+alias+" in your query should not be in the PARTITIONS clause.  Delete it!!!!");
		else if(child.getChildCount() > 1) {
			CommonTree columnNode = (CommonTree) child.getChild(1);
			String nameOfColumn = columnNode.getText();
			String withoutQuotes = nameOfColumn.substring(1, nameOfColumn.length()-1);	
			partitionColumn = findColumn(partitionedColumns, withoutQuotes);
			if(partitionColumn == null)
				throw new IllegalArgumentException("The meta data specified column="+nameOfColumn+" of having the " +
						"@NoSqlPartitionByThisField but that is not true(at least metadata in database does " +
						"not have that so pick a different column)");
		} else if(partitionedColumns.size() > 1)
			throw new IllegalArgumentException("The meta data/Annotations contains MORE than one partition, so you have to specify something like "+alias+"('<columnThatWePartitionedBy>', :partId)");
		else {
			partitionColumn = partitionedColumns.get(0);
		}
		
		CommonTree partitionIdNode = (CommonTree) child.getChild(0);
		TypeInfo typeInfo = new TypeInfo(partitionColumn);
		ExpressionNode node = new ExpressionNode(partitionIdNode);
		processSide(node, wiring, typeInfo, facade);

		PartitionMeta p = new PartitionMeta(partitionColumn, node);
		info.setPartition(p);
	}
	
	private DboColumnMeta findColumn(List<DboColumnMeta> partitionedColumns, String nameOfColumn) {
		for(DboColumnMeta colMeta : partitionedColumns) {
			if(nameOfColumn.equals(colMeta.getColumnName()))
				return colMeta;
		}
		return null;
	}

	@SuppressWarnings({ "unchecked" })
	private <T> void compileFromClause(CommonTree tree,
			InfoForWiring wiring, MetaFacade facade) {
		List<CommonTree> childrenList = tree.getChildren();
		if (childrenList == null)
			return;
		
		for (CommonTree child : childrenList) {
			int type = child.getType();
			switch (type) {
			case NoSqlLexer.TABLE_NAME:
				loadTableIntoWiringInfo(wiring, child, facade);
				break;
			default:
				break;
			}
		}
	}

	private <T> void loadTableIntoWiringInfo(InfoForWiring wiring, CommonTree tableNode, MetaFacade facade) {
		// What should we add to metaQuery here
		// AND later when we do joins, we need to tell the factory
		// here as well
		String tableName = tableNode.getText();
		String targetTable = wiring.getTargetTable();
		DboTableMeta metaClass = findTable(facade, tableName);
		
		//NOTE: special case for ORM layer only NOT for ad-hoc query!!!
		if(tableName.equals("TABLE") && targetTable != null) {
			metaClass = findTable(facade, targetTable);
		} else if(metaClass == null)
			throw new IllegalArgumentException("Meta data(or Entity)="+tableName+" cannot be found");

		ViewInfoImpl info = new ViewInfoImpl(null, metaClass);
		
		if(tableNode.getChildCount() == 0) {
			if(wiring.getNoAliasTable() != null)
				throw new IllegalArgumentException("This query has two tables with no alias.  This is not allowed");
			wiring.setNoAliasTable(info);
		} else {
			CommonTree aliasNode = (CommonTree) tableNode.getChildren().get(0);
			String alias = aliasNode.getText();
			wiring.putAliasTable(alias, info);
			info.setAlias(alias);
		}

		wiring.addEagerlyJoinedView(info);
		
		//set the very first table as the target table
		if(wiring.getMetaQueryTargetTable() != null)
			throw new RuntimeException("Two tables are not supported at this time.");
		
		wiring.setMetaQueryTargetTable(metaClass);
	}

	private DboTableMeta findTable(MetaFacade facade, String tableName) {
		DboTableMeta metaClass = facade.getColumnFamily(tableName);
		return metaClass;
	}
	
	@SuppressWarnings("unchecked")
	private static <T> void compileSelectClause(CommonTree tree,
			InfoForWiring wiring) {

		List<CommonTree> childrenList = tree.getChildren();
		if (childrenList != null && childrenList.size() > 0) {
			for (CommonTree child : childrenList) {
				switch (child.getType()) {
				case NoSqlLexer.SELECT_RESULTS:
					parseSelectResults(child, wiring);
					break;
				default:
					break;
				}
			}
		}
	}

	// the alias part is silly due to not organize right in .g file
	@SuppressWarnings({ "unchecked" })
	private static <T> void parseSelectResults(CommonTree tree,
			InfoForWiring wiring) {
		List<CommonTree> childrenList = tree.getChildren();
		if (childrenList == null)
			return;
			
		for (CommonTree child : childrenList) {
			switch (child.getType()) {
			case NoSqlLexer.STAR:
				wiring.setSelectStarDefined(true);
				break;
			case NoSqlLexer.ATTR_NAME:
				
				String columnNameOrAlias = child.getText();
				ViewInfoImpl info = wiring.getInfoFromAlias(columnNameOrAlias);
				if(info != null) {
					wiring.setSelectStarDefined(true);
					continue;
				}
				
				String alias = null;
				List<CommonTree> children = child.getChildren();
				if(children == null) //It must be an alias if there are no children!!!
					throw new IllegalArgumentException("You have an alias of="+columnNameOrAlias+" that does not exist in the FROM part of the select statement.  query="+wiring.getQuery());
				if(children.size() > 0) { //we have an alias too!!!
					CommonTree aliasNode = children.get(0);
					alias = aliasNode.getText();
					
					String fullName = alias+"."+columnNameOrAlias;
					if(wiring.getInfoFromAlias(alias) == null)
						throw new RuntimeException("The select portion has an attribute="+fullName+" with an alias that was not defined in the from section");					
				} else {
					//later made need to add attributeName information to metaQuery here
					
					if(wiring.getNoAliasTable() == null)
						throw new RuntimeException("The select portion has an attribute="+columnNameOrAlias+" with no alias but there is no table with no alias in from section");					
				}
				
				break;
			case NoSqlLexer.ALIAS:
				break;

			default:
				break;
			}
		}
	}

	@SuppressWarnings("unchecked")
	private static <T> void compileExpression(ExpressionNode node, InfoForWiring wiring, MetaFacade facade) {
		CommonTree expression = node.getASTNode();
		int type = expression.getType();
		log.debug("where type:" + expression.getType());
		switch (type) {
		case NoSqlLexer.AND:
		case NoSqlLexer.OR:
			node.setState("ANDORnode", null);
			List<CommonTree> children = expression.getChildren();
			processSide(node, wiring, children, 0, ChildSide.LEFT, facade);
			processSide(node, wiring, children, 1, ChildSide.RIGHT, facade);
			
			break;
		case NoSqlLexer.EQ:
		case NoSqlLexer.NE:
		case NoSqlLexer.GT:
		case NoSqlLexer.LT:
		case NoSqlLexer.GE:
		case NoSqlLexer.LE:
			compileComparator(node, wiring, facade, expression);
			break;
		case NoSqlLexer.BETWEEN:
			throw new UnsupportedOperationException("not supported yet, use <= and >= intead for now");
		default:
			break;
		}
	}

	private static void compileComparator(ExpressionNode node,
			InfoForWiring wiring, MetaFacade facade, CommonTree expression) {

		//The right side could be value/constant or variable or true or false, or decimal, etc. etc.
		CommonTree leftSide = (CommonTree) expression.getChild(0);
		CommonTree rightSide = (CommonTree) expression.getChild(1);
		
		//This is a VERY difficult issue.  We basically want the type information
		//first either from the constant OR from the column name, then we want to use
		//that NOW or later to verify the type of the other side of the equation, BUT which
		//one has the type information as he needs to go first.  If both are parameters
		//that are passed in, we have no type information, correct?  :name and :something
		//could be any types and may not match at all so first let's disallow param to param
		//matching since developers can do that BEFORE they run the query anyways in the 
		//java code.  ie. FIRST, let's find the side with type information
		
		//screw it, we deleted that code and force you to have one side be a column for now!!!!
		if(isAttribute(rightSide)) {
			expression.setChild(0, rightSide);
			expression.setChild(1, leftSide);
			CommonTree temp = rightSide;
			rightSide = leftSide;
			leftSide = temp;				
		} else if(!isAttribute(leftSide)) {
			throw new IllegalArgumentException("Currently, each param in the where clause must be compared to an attribute.  bad query="+wiring.getQuery()+" bad piece="+node);
		}
		
		ExpressionNode left  = new ExpressionNode(leftSide);
		ExpressionNode right = new ExpressionNode(rightSide);
		node.setChild(ChildSide.LEFT, left);
		node.setChild(ChildSide.RIGHT, right);
		
		TypeInfo typeInfo = processSide(left, wiring, null, facade);
		processSide(right, wiring, typeInfo, facade);
		
		Object state = left.getState();
		if(state instanceof StateAttribute) {
			StateAttribute st = (StateAttribute) state;
			ViewInfoImpl tableInfo = st.getViewInfo();
			node.setState(tableInfo, null);
		}
	}

	private static <T> ExpressionNode processSide(ExpressionNode node, InfoForWiring wiring,
			List<CommonTree> children, int i, ChildSide side, MetaFacade facade) {
		CommonTree child = children.get(i);
		ExpressionNode childNode = new ExpressionNode(child);
		node.setChild(side, childNode);
		compileExpression(childNode, wiring, facade);
		return childNode;
	}

	
	private static boolean isAttribute(CommonTree node) {
		if(node.getType() == NoSqlLexer.ATTR_NAME)
			return true;
		return false;
	}

//	private static boolean hasTypeInfo(CommonTree node) {
//		if(node.getType() == NoSqlLexer.ATTR_NAME || node.getType() == NoSqlLexer.DECIMAL
//				|| node.getType() == NoSqlLexer.INT_VAL || node.getType() == NoSqlLexer.STR_VAL)
//			return true;
//		return false;
//	}

	private static TypeInfo processSide(ExpressionNode node, InfoForWiring wiring, TypeInfo typeInfo, MetaFacade facade) {
		if(node.getType() == NoSqlLexer.ATTR_NAME) {
			return processColumnName(node, wiring, typeInfo, facade);
		} else if(node.isParameter()) {
			return processParam(node, wiring, typeInfo);
		} else if(node.isConstant()) {
			return processConstant(node, wiring, typeInfo);
		} else 
			throw new RuntimeException("bug, type not supported yet="+node.getType());
	}

	private static TypeInfo processConstant(ExpressionNode node, InfoForWiring wiring, TypeInfo typeInfo) {
		String constant = node.getASTNode().getText();
		
		StorageTypeEnum ourType;
		if(node.getType() == NoSqlLexer.DECIMAL){
			ourType = StorageTypeEnum.DECIMAL;
			BigDecimal dec = new BigDecimal(constant);
			node.setState(dec, constant); 
		} else if(node.getType() == NoSqlLexer.STR_VAL){
			String withoutQuotes = constant.substring(1, constant.length()-1);		
			ourType = StorageTypeEnum.STRING;
			node.setState(withoutQuotes, constant);
		} else if(node.getType() == NoSqlLexer.INT_VAL){
			ourType = StorageTypeEnum.INTEGER;
			BigInteger bigInt = new BigInteger(constant);
			node.setState(bigInt, constant);
		} else if(node.getType() == NoSqlLexer.BOOL_VAL) {
			ourType = StorageTypeEnum.BOOLEAN;
			boolean boolVal = Boolean.parseBoolean(constant);
			node.setState(boolVal, constant);
		} else if(node.getType() == NoSqlLexer.NULL) {
			ourType = StorageTypeEnum.NULL;
			node.setState(null, constant);
		}
			
		else 
			throw new RuntimeException("bug, not supported type(please fix)="+node.getType());
		
		if(typeInfo == null) //no types to check against so return...
			return new TypeInfo(ourType);

		//we must compare type info so this next stuff is pure validation
		if(typeInfo.getColumnInfo() != null) {
			validateTypes(wiring, ourType, typeInfo);
		} else {
			StorageTypeEnum constantType = typeInfo.getConstantType();
			if(constantType != ourType)
				throw new IllegalArgumentException("Types do not match in namedquery="+wiring.getQuery());
		}
		return null;
	}

	private static void validateTypes(InfoForWiring wiring, StorageTypeEnum constantType, TypeInfo typeInfo) {
		DboColumnMeta info = typeInfo.getColumnInfo();
		if(info instanceof DboColumnToManyMeta)
			throw new IllegalArgumentException("Cannot use column="+info.getColumnName()+" since that is a toMany relationship");
		else if(constantType == StorageTypeEnum.NULL)
			return; //null is any type so no need to match
		else if(constantType != info.getStorageType())
			throw new IllegalArgumentException("Types do not match in namedquery="+wiring.getQuery()+" for column="+info.getColumnName()+" type1="+constantType+" type2="+info.getStorageType());
	}
	

	private static TypeInfo processColumnName( 
			ExpressionNode attributeNode2, InfoForWiring wiring, TypeInfo otherSideType, MetaFacade facade) {
		ViewInfoImpl tableInfo;
		
		CommonTree colNameNode = attributeNode2.getASTNode();
		String columnName = colNameNode.getText();
		String textInSql = columnName;
		if (colNameNode.getChildCount() > 0) {
			String aliasEntity = colNameNode.getChild(0).getText();
			
			tableInfo = wiring.getInfoFromAlias(aliasEntity);
			textInSql = aliasEntity+"."+columnName;
			if(tableInfo == null)
				throw new RuntimeException("The where clause attribute="
						+textInSql+" has an alias that does not exist in from clause");
		} else {
			tableInfo = wiring.getNoAliasTable();
			if(tableInfo == null)
				throw new RuntimeException("The where clause attribute="
						+textInSql+" has no alias and from clause only has tables with alias");
		}
		
		DboTableMeta metaClass = tableInfo.getTableMeta();
		//At this point, we have looked up the metaClass associated with the alias
		DboColumnMeta colMeta = facade.getColumnMeta(metaClass, columnName);
		if (colMeta == null) {
			//okay, there is no column found, but maybe the column name for the id matches(id is a special case)
			colMeta = metaClass.getIdColumnMeta();
			if(!colMeta.getColumnName().equals(columnName)) {
				List<String> names = metaClass.getColumnNameList();
				throw new IllegalArgumentException("There is no column=" + columnName + " that exists for table " 
						+ metaClass.getColumnFamily()+" potential columns are="+names+"  rowkey col="+colMeta.getColumnName());
			}
		}
		
		
		wiring.addEagerlyJoinedView(tableInfo);
		StateAttribute attr = new StateAttribute(tableInfo, colMeta, textInSql); 
		attributeNode2.setState(attr, textInSql);
		wiring.incrementAttributesCount(textInSql);
		
		TypeInfo typeInfo = new TypeInfo(colMeta);
		
		if(otherSideType == null)
			return typeInfo;
		
		if(otherSideType.getConstantType() != null) {
			validateTypes(wiring, otherSideType.getConstantType(), typeInfo);
		} else {
			Class<?> classType = otherSideType.getColumnInfo().getClassType();
			if(!classType.equals(colMeta.getClassType()))
				throw new IllegalArgumentException("Types are not the same for query="+wiring.getQuery()+" types="+classType+" and "+colMeta.getClassType());
		}

		return null;
	}
	
	private static TypeInfo processParam(ExpressionNode parameterNode2, InfoForWiring wiring, TypeInfo typeInfo) {
		CommonTree parameterNode = parameterNode2.getASTNode();
		String parameter = parameterNode.getText();
		
		wiring.getParameterFieldMap().put(parameter, typeInfo);
		
		parameterNode2.setState(parameter, ":"+parameter);
		return null;
	}	
}
