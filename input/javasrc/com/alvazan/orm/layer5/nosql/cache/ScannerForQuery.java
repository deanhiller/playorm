package com.alvazan.orm.layer5.nosql.cache;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.inject.Inject;
import javax.inject.Provider;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.exc.ParseException;
import com.alvazan.orm.api.spi3.meta.DboColumnMeta;
import com.alvazan.orm.api.spi3.meta.DboColumnToManyMeta;
import com.alvazan.orm.api.spi3.meta.DboColumnToOneMeta;
import com.alvazan.orm.api.spi3.meta.DboDatabaseMeta;
import com.alvazan.orm.api.spi3.meta.DboTableMeta;
import com.alvazan.orm.api.spi3.meta.MetaQuery;
import com.alvazan.orm.api.spi3.meta.StorageTypeEnum;
import com.alvazan.orm.api.spi3.meta.TypeInfo;
import com.alvazan.orm.layer5.indexing.ExpressionNode;
import com.alvazan.orm.layer5.indexing.JoinType;
import com.alvazan.orm.layer5.indexing.SpiMetaQueryImpl;
import com.alvazan.orm.layer5.indexing.StateAttribute;
import com.alvazan.orm.layer5.indexing.TableInfo;
import com.alvazan.orm.parser.antlr.NoSqlLexer;
import com.alvazan.orm.parser.antlr.NoSqlParser;
import com.alvazan.orm.parser.antlr.ParseQueryException;

@SuppressWarnings("rawtypes")
public class ScannerForQuery {

	private static final Logger log = LoggerFactory
			.getLogger(ScannerForQuery.class);
	@Inject
	private Provider<SpiMetaQueryImpl> factory;
	@Inject
	private DboDatabaseMeta metaInfo;
	
	@Inject
	private Provider<MetaQuery> metaQueryFactory;

	/**
	 * For ad-hoc query tool only
	 * @param query
	 * @return
	 */
	public MetaQuery parseQuery(String query, Object mgr) {
		return newsetupByVisitingTree(query, null, mgr, "Query="+query+". ");
	}
	
	public MetaQuery newsetupByVisitingTree(String query, String targetTable, Object mgr, String errorMsg) {
		try {
			return newsetupByVisitingTreeImpl(query, targetTable, mgr, errorMsg);
		} catch(ParseQueryException e) {
			String msg = errorMsg+"  failed to parse.  Specific reason="+e.getMessage();
			throw new ParseException(msg, e);
		} catch(RuntimeException e) {
			throw new ParseException(errorMsg+" failed to compile.  See chained exception for cause", e);
		}
	}
	
	@SuppressWarnings({ "unchecked" })
	private MetaQuery newsetupByVisitingTreeImpl(String query, String targetTable, Object mgr, String errorMsg) {
		CommonTree theTree = parseTree(query);
		MetaQuery visitor1 = metaQueryFactory.get();
		SpiMetaQueryImpl spiMetaQuery = factory.get(); 
		
		visitor1.initialize(query, spiMetaQuery);

		InfoForWiring wiring = new InfoForWiring(query, targetTable, (NoSqlEntityManager) mgr);
		
		// VISITOR PATTERN(if you don't know that pattern, google it!!!)
		// Normally, the visitor is injected INTO theTree variable itself but I
		// am too lazy to
		// figure out the code generation of antLR to do that plus that might
		// not be as flexible as this
		// anyways since more people can make changes without the grammar
		// knowledge this way
		walkTheTree(theTree, visitor1, wiring);

		ExpressionNode node = wiring.getAstTree();
		ExpressionNode newTree = rewireTreeIfNeededForBetween(node, wiring.getAttributeUsedCount(), query);
		
		spiMetaQuery.setASTTree(newTree, wiring.getFirstTable(), wiring.getTables());
		
		return visitor1;
	}
	
	private ExpressionNode rewireTreeIfNeededForBetween(ExpressionNode node,
			Map<String, Integer> attributeUsedCount, String query) {
		ExpressionNode root = node;
		for(Entry<String, Integer> m : attributeUsedCount.entrySet()) {
			if(m.getValue().intValue() <= 1)
				continue;
			
			log.info("optimizing query tree for varname="+m.getKey());
			BetweenVisitor visitor = new BetweenVisitor(m.getKey());
			root = visitor.walkAndFixTree(root, query);
		}
		
		return root;
	}

	public static CommonTree parseTree(String query) {
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
	private <T> void walkTheTree(CommonTree tree, MetaQuery<T> metaQuery, InfoForWiring wiring) {
		int type = tree.getType();
		switch (type) {
		//The tree should come in with this order OR we are in BIG TROUBLE as we need the information
		//in the from clause first to get alias and what table the alias maps too
		case NoSqlLexer.FROM_CLAUSE:
			compileFromClause(tree, metaQuery, wiring);
			break;
		case NoSqlLexer.JOIN_CLAUSE:
			compileJoinClause(tree, metaQuery, wiring);
			break;
		case NoSqlLexer.PARTITIONS_CLAUSE:
			compilePartitionsClause(tree, metaQuery, wiring);
			break;
		case NoSqlLexer.SELECT_CLAUSE:
			compileSelectClause(tree, metaQuery, wiring);
			break;
		case NoSqlLexer.WHERE:
			//We should try to get rid of the where token in the grammar so we don't need 
			//this line of code here....
			CommonTree expression = (CommonTree)tree.getChildren().get(0);
			ExpressionNode node = new ExpressionNode(expression);
			compileExpression(node, metaQuery, wiring);
			wiring.setAstTree(node);
			break;

		case 0: // nil
			List<CommonTree> childrenList = tree.getChildren();
			for (CommonTree child : childrenList) {
				walkTheTree(child, metaQuery, wiring);
			}
			break;
		default:
			break;
		}
	}

	private <T> void compileJoinClause(CommonTree tree,
			MetaQuery<T> metaQuery, InfoForWiring wiring) {
		List children = tree.getChildren();
		
		for(Object child : children) {
			CommonTree node = (CommonTree) child;
			int type = node.getType();
			if(type == NoSqlLexer.LEFT_OUTER_JOIN) {
				throw new UnsupportedOperationException("do not yet support left out join, please let me know and I can implement quickly");
			} else if(type == NoSqlLexer.INNER_JOIN) {
				compileJoin(node, metaQuery, wiring, JoinType.INNER);
			} else
				throw new UnsupportedOperationException("bug?, type="+type+" and we don't process that type for joins");
		}
	}
	
	private <T> void compileJoin(CommonTree tree, MetaQuery<T> metaQuery, InfoForWiring wiring, JoinType type) {
		List children = tree.getChildren();
		CommonTree aliasedColumn = (CommonTree) children.get(0);
		CommonTree aliasNode = (CommonTree) aliasedColumn.getChild(0);
		CommonTree newAliasNode = (CommonTree) children.get(1);
		String column = aliasedColumn.getText();
		String alias = aliasNode.getText();
		String newAlias = newAliasNode.getText();
		
		TableInfo tableInfo = wiring.getInfoFromAlias(alias);
		DboTableMeta tableMeta = tableInfo.getTableMeta();
		DboColumnMeta columnMeta = tableMeta.getColumnMeta(column);
		if(!(columnMeta instanceof DboColumnToOneMeta))
			throw new IllegalArgumentException("Column="+column+" on table="+tableMeta.getColumnFamily()+" is NOT a OneToOne NOR ManyToOne relationship according to our meta data");
		
		DboColumnToOneMeta toOne = (DboColumnToOneMeta) columnMeta;
		DboTableMeta fkTableMeta = toOne.getFkToColumnFamily();

		TableInfo fkInfo = new TableInfo(fkTableMeta, JoinType.INNER);
		tableInfo.putJoinTable(column, fkInfo);
		wiring.putAliasTable(newAlias, fkInfo);
	}
	
	@SuppressWarnings("unchecked")
	private <T> void compilePartitionsClause(CommonTree tree,
			MetaQuery<T> metaQuery, InfoForWiring wiring) {
		List<CommonTree> childrenList = tree.getChildren();
		for(CommonTree child : childrenList) {
			compileSinglePartition(metaQuery, wiring, child);
		}
	}

	private void compileSinglePartition(MetaQuery metaQuery, InfoForWiring wiring, CommonTree child) {
		int type = child.getType();
		String alias = child.getText();
		TableInfo info = wiring.getInfoFromAlias(alias);
		if(info == null)
			throw new IllegalArgumentException("In your PARTITIONS clause, you have an alias='"+alias+"' that is not found in your FROM clause");
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
			throw new IllegalArgumentException("The meta data contains MORE than one partition, so you have to specify something like "+alias+"('<columnThatWePartitionedBy>', :partId)");
		else {
			partitionColumn = partitionedColumns.get(0);
		}
		
		CommonTree partitionIdNode = (CommonTree) child.getChild(0);
		TypeInfo typeInfo = new TypeInfo(partitionColumn);
		ExpressionNode node = new ExpressionNode(partitionIdNode);
		processSide(metaQuery, node, wiring, typeInfo);

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
			MetaQuery<T> metaQuery, InfoForWiring wiring) {
		List<CommonTree> childrenList = tree.getChildren();
		if (childrenList == null)
			return;
		
		for (CommonTree child : childrenList) {
			int type = child.getType();
			switch (type) {
			case NoSqlLexer.TABLE_NAME:
				loadTableIntoWiringInfo(metaQuery, wiring, child);
				break;
			default:
				break;
			}
		}
	}

	private <T> void loadTableIntoWiringInfo(MetaQuery<T> metaQuery,
			InfoForWiring wiring, CommonTree tableNode) {
		// What should we add to metaQuery here
		// AND later when we do joins, we need to tell the factory
		// here as well
		String tableName = tableNode.getText();
		String targetTable = wiring.getTargetTable();
		DboTableMeta metaClass = findTable(wiring, tableName);
		
		//NOTE: special case for ORM layer only NOT for ad-hoc query!!!
		if(tableName.equals("TABLE") && targetTable != null) {
			metaClass = findTable(wiring, targetTable);
		} else if(metaClass == null)
			throw new IllegalArgumentException("Query="+metaQuery+" failed to parse.  entity="+tableName+" cannot be found");
			
		TableInfo info = new TableInfo(metaClass, JoinType.NONE);
		if(wiring.getFirstTable() == null)
			wiring.setFirstTable(info);
		
		if(tableNode.getChildCount() == 0) {
			if(wiring.getNoAliasTable() != null)
				throw new IllegalArgumentException("Query="+metaQuery+" has two tables with no alias.  This is not allowed");
			wiring.setNoAliasTable(info);
		} else {
			CommonTree aliasNode = (CommonTree) tableNode.getChildren().get(0);
			String alias = aliasNode.getText();
			wiring.putAliasTable(alias, info);
		}
		
		//set the very first table as the target table
		if(metaQuery.getTargetTable() != null)
			throw new RuntimeException("Two tables are not supported at this time.  query="+metaQuery);
		
		metaQuery.setTargetTable(metaClass);
	}

	private DboTableMeta findTable(InfoForWiring wiring, String tableName) {
		DboTableMeta metaClass = metaInfo.getMeta(tableName);
		if(metaClass == null && wiring.getMgr() != null)
			metaClass = wiring.getMgr().find(DboTableMeta.class, tableName);
		return metaClass;
	}
	
	@SuppressWarnings("unchecked")
	private static <T> void compileSelectClause(CommonTree tree,
			MetaQuery<T> metaQuery, InfoForWiring wiring) {

		List<CommonTree> childrenList = tree.getChildren();
		if (childrenList != null && childrenList.size() > 0) {
			for (CommonTree child : childrenList) {
				switch (child.getType()) {
				case NoSqlLexer.SELECT_RESULTS:
					parseSelectResults(child, metaQuery, wiring);
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
			MetaQuery<T> metaQuery, InfoForWiring wiring) {
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
				TableInfo info = wiring.getInfoFromAlias(columnNameOrAlias);
				if(info != null) {
					wiring.setSelectStarDefined(true);
					continue;
				}
				
				String alias = null;
				List children = child.getChildren();
				if(children == null) //It must be an alias if there are no children!!!
					throw new IllegalArgumentException("You have an alias of="+columnNameOrAlias+" that does not exist in the FROM part of the select statement.  query="+wiring.getQuery());
				if(children.size() > 0) { //we have an alias too!!!
					CommonTree aliasNode = (CommonTree) child.getChildren().get(0);
					alias = aliasNode.getText();
					
					String fullName = alias+"."+columnNameOrAlias;
					if(wiring.getInfoFromAlias(alias) == null)
						throw new RuntimeException("query="+metaQuery+" in the select portion has an attribute="+fullName+" with an alias that was not defined in the from section");					
				} else {
					//later made need to add attributeName information to metaQuery here
					
					if(wiring.getNoAliasTable() == null)
						throw new RuntimeException("query="+metaQuery+" in the select portion has an attribute="+columnNameOrAlias+" with no alias but there is no table with no alias in from section");					
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
	private static <T> void compileExpression(ExpressionNode node,
			MetaQuery<T> metaQuery, InfoForWiring wiring) {
		CommonTree expression = node.getASTNode();
		int type = expression.getType();
		log.debug("where type:" + expression.getType());
		switch (type) {
		case NoSqlLexer.AND:
		case NoSqlLexer.OR:
			node.setState("ANDORnode");
			List<CommonTree> children = expression.getChildren();
			for(int i = 0; i < 2; i++) {
				CommonTree child = children.get(i);
				ExpressionNode childNode = new ExpressionNode(child);
				if(i == 0)
					node.setLeftChild(childNode);
				else if(i == 1)
					node.setRightChild(childNode);
				else
					throw new RuntimeException("We have a big problem, we don't have a binary tree anymore");
				
				compileExpression(childNode, metaQuery, wiring);
				
			}
			break;
		case NoSqlLexer.EQ:
		case NoSqlLexer.NE:
		case NoSqlLexer.GT:
		case NoSqlLexer.LT:
		case NoSqlLexer.GE:
		case NoSqlLexer.LE:
			node.setState("comparatorNode");
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
			node.setLeftChild(left);
			node.setRightChild(right);
			
			TypeInfo typeInfo = processSide(metaQuery, left, wiring, null);
			processSide(metaQuery, right, wiring, typeInfo);
			
			break;
		default:
			break;
		}
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

	private static TypeInfo processSide(MetaQuery metaQuery, ExpressionNode node, InfoForWiring wiring, TypeInfo typeInfo) {
		if(node.getType() == NoSqlLexer.ATTR_NAME) {
			return processColumnName(metaQuery, node, wiring, typeInfo);
		} else if(node.getType() == NoSqlLexer.PARAMETER_NAME) {
			return processParam(metaQuery, node, wiring, typeInfo);
		} else if(node.getType() == NoSqlLexer.DECIMAL || node.getType() == NoSqlLexer.STR_VAL
				|| node.getType() == NoSqlLexer.INT_VAL) {
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
			node.setState(dec); 
		}
		else if(node.getType() == NoSqlLexer.STR_VAL){
			String withoutQuotes = constant.substring(1, constant.length()-1);		
			ourType = StorageTypeEnum.STRING;
			node.setState(withoutQuotes);
		}
		else if(node.getType() == NoSqlLexer.INT_VAL){
			ourType = StorageTypeEnum.INTEGER;
			BigInteger bigInt = new BigInteger(constant);
			node.setState(bigInt);
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
		else if(constantType != info.getStorageType())
			throw new IllegalArgumentException("Types do not match in namedquery="+wiring.getQuery()+" for column="+info.getColumnName()+" type1="+constantType+" type2="+info.getStorageType());
	}
	

	private static TypeInfo processColumnName(MetaQuery metaQuery, 
			ExpressionNode attributeNode2, InfoForWiring wiring, TypeInfo otherSideType) {
		TableInfo tableInfo;
		
		CommonTree colNameNode = attributeNode2.getASTNode();
		String columnName = colNameNode.getText();
		String textInSql = columnName;
		if (colNameNode.getChildCount() > 0) {
			String aliasEntity = colNameNode.getChild(0).getText();
			
			tableInfo = wiring.getInfoFromAlias(aliasEntity);
			textInSql = aliasEntity+"."+columnName;
			if(tableInfo == null)
				throw new RuntimeException("query="+metaQuery+" failed to parse because in where clause attribute="
						+textInSql+" has an alias that does not exist in from clause");
		} else {
			tableInfo = wiring.getNoAliasTable();
			if(tableInfo == null)
				throw new RuntimeException("query="+metaQuery+" failed to parse because in where clause attribute="
						+textInSql+" has no alias and from clause only has tables with alias");
		}
		
		DboTableMeta metaClass = tableInfo.getTableMeta();
		//At this point, we have looked up the metaClass associated with the alias
		DboColumnMeta colMeta = metaClass.getColumnMeta(columnName);
		if (colMeta == null) {
			//okay, there is no column found, but maybe the column name for the id matches(id is a special case)
			colMeta = metaClass.getIdColumnMeta();
			if(!colMeta.getColumnName().equals(columnName))
				throw new IllegalArgumentException("There is no " + columnName + " exists for class " + metaClass);
		}
		
		if(!colMeta.isIndexed())
			throw new IllegalArgumentException("You cannot have '"+textInSql+"' in your sql query since "+columnName+" has no @Index annotation on the field in the entity");
		
		StateAttribute attr = new StateAttribute(metaClass.getColumnFamily(), colMeta); 
		attributeNode2.setState(attr);
		wiring.incrementAttributesCount(metaClass.getColumnFamily()+"-"+colMeta.getColumnName());
		
		TypeInfo typeInfo = new TypeInfo(colMeta);
		
		if(otherSideType == null)
			return typeInfo;
		
		if(otherSideType.getConstantType() != null) {
			validateTypes(wiring, otherSideType.getConstantType(), typeInfo);
		} else {
			Class classType = otherSideType.getColumnInfo().getClassType();
			if(!classType.equals(colMeta.getClassType()))
				throw new IllegalArgumentException("Types are not the same for query="+wiring.getQuery()+" types="+classType+" and "+colMeta.getClassType());
		}

		return null;
	}
	
	@SuppressWarnings("unchecked")
	private static TypeInfo processParam(MetaQuery metaQuery, 
			ExpressionNode parameterNode2, InfoForWiring wiring, TypeInfo typeInfo) {
		CommonTree parameterNode = parameterNode2.getASTNode();
		String parameter = parameterNode.getText();
		
		metaQuery.getParameterFieldMap().put(parameter, typeInfo);
		
		parameterNode2.setState(parameter);
		return null;
	}

}
