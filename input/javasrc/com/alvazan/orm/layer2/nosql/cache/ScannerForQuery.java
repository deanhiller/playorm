package com.alvazan.orm.layer2.nosql.cache;

import java.util.List;

import javax.inject.Inject;
import javax.inject.Provider;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.spi2.DboColumnMeta;
import com.alvazan.orm.api.spi2.DboDatabaseMeta;
import com.alvazan.orm.api.spi2.DboTableMeta;
import com.alvazan.orm.api.spi2.MetaQuery;
import com.alvazan.orm.api.spi2.StorageTypeEnum;
import com.alvazan.orm.api.spi2.TypeInfo;
import com.alvazan.orm.layer2.indexing.ExpressionNode;
import com.alvazan.orm.layer2.indexing.SpiMetaQueryImpl;
import com.alvazan.orm.layer2.indexing.StateAttribute;
import com.alvazan.orm.parser.antlr.NoSqlLexer;
import com.alvazan.orm.parser.antlr.NoSqlParser;

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
	public MetaQuery parseQuery(String query) {
		return newsetupByVisitingTree(query, null);
	}
	
	@SuppressWarnings({ "unchecked" })
	public MetaQuery newsetupByVisitingTree(String query, String targetTable) {
		CommonTree theTree = parseTree(query);
		MetaQuery visitor1 = metaQueryFactory.get();
		SpiMetaQueryImpl spiMetaQuery = factory.get(); 
		
		visitor1.initialize(query, spiMetaQuery);

		InfoForWiring wiring = new InfoForWiring(query, targetTable);
		
		// VISITOR PATTERN(if you don't know that pattern, google it!!!)
		// Normally, the visitor is injected INTO theTree variable itself but I
		// am too lazy to
		// figure out the code generation of antLR to do that plus that might
		// not be as flexible as this
		// anyways since more people can make changes without the grammar
		// knowledge this way
		walkTheTree(theTree, visitor1, wiring);

		spiMetaQuery.setASTTree(wiring.getAstTree());
		
		return visitor1;
	}
	
	public static CommonTree parseTree(String query) {
        ANTLRStringStream stream = new ANTLRStringStream(query);
        NoSqlLexer lexer = new NoSqlLexer(stream);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        NoSqlParser parser = new NoSqlParser(tokenStream);

        try {
            return (CommonTree) parser.selectStatement().getTree();
        } catch (RecognitionException re) {
            throw new RuntimeException(query, re);
        }
    }
	
	@SuppressWarnings("unchecked")
	private <T> void walkTheTree(CommonTree tree, MetaQuery<T> metaQuery, InfoForWiring wiring) {
		int type = tree.getType();
		switch (type) {
		case NoSqlLexer.FROM_CLAUSE:
			parseFromClause(tree, metaQuery, wiring);
			break;
		case NoSqlLexer.SELECT_CLAUSE:
			parseSelectClause(tree, metaQuery, wiring);
			break;
		case NoSqlLexer.WHERE:
			//We should try to get rid of the where token in the grammar so we don't need 
			//this line of code here....
			CommonTree expression = (CommonTree)tree.getChildren().get(0);
			ExpressionNode node = new ExpressionNode(expression);
			parseExpression(node, metaQuery, wiring);
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

	@SuppressWarnings("unchecked")
	private static <T> void parseSelectClause(CommonTree tree,
			MetaQuery<T> metaQuery, InfoForWiring wiring) {

		List<CommonTree> childrenList = tree.getChildren();
		if (childrenList != null && childrenList.size() > 0) {
			for (CommonTree child : childrenList) {
				switch (child.getType()) {
				case NoSqlLexer.RESULT:
					parseResult(child, metaQuery, wiring);
					break;
				default:
					break;
				}
			}
		}
	}

	// the alias part is silly due to not organize right in .g file
	@SuppressWarnings({ "unchecked" })
	private static <T> void parseResult(CommonTree tree,
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
				
				String attributeName = child.getText();
				DboTableMeta meta = wiring.getInfoFromAlias(attributeName);
				if(meta != null) {
					wiring.setSelectStarDefined(true);
					continue;
				}
					
				String alias = null;
				if(child.getChildren().size() > 0) { //we have an alias too!!!
					CommonTree aliasNode = (CommonTree) child.getChildren().get(0);
					alias = aliasNode.getText();
					
					String fullName = alias+"."+attributeName;
					if(wiring.getInfoFromAlias(alias) == null)
						throw new RuntimeException("query="+metaQuery+" in the select portion has an attribute="+fullName+" with an alias that was not defined in the from section");					
				} else {
					//later made need to add attributeName information to metaQuery here
					
					if(wiring.getNoAliasTable() == null)
						throw new RuntimeException("query="+metaQuery+" in the select portion has an attribute="+attributeName+" with no alias but there is no table with no alias in from section");					
				}
				
				break;
			case NoSqlLexer.ALIAS:
				break;

			default:
				break;
			}
		}
	}

	@SuppressWarnings({ "unchecked" })
	private <T> void parseFromClause(CommonTree tree,
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
		DboTableMeta metaClass = metaInfo.getMeta(tableName);
		//NOTE: special case for ORM layer only NOT for ad-hoc query!!!
		if(tableName.equals("TABLE") && targetTable != null) {
			metaClass = metaInfo.getMeta(targetTable);
		} else if(metaClass == null)
			throw new IllegalArgumentException("Query="+metaQuery+" failed to parse.  entity="+tableName+" cannot be found");
			
		if(tableNode.getChildCount() == 0) {
			if(wiring.getNoAliasTable() != null)
				throw new IllegalArgumentException("Query="+metaQuery+" has two tables with no alias.  This is not allowed");
			wiring.setNoAliasTable(metaClass);
		} else {
			CommonTree aliasNode = (CommonTree) tableNode.getChildren().get(0);
			String alias = aliasNode.getText();
			wiring.put(alias, metaClass);
		}
		
		//set the very first table as the target table
		if(metaQuery.getTargetTable() != null)
			throw new RuntimeException("Two tables are not supported at this time.  query="+metaQuery);
		
		metaQuery.setTargetTable(metaClass);
	}

	@SuppressWarnings("unchecked")
	private static <T> void parseExpression(ExpressionNode node,
			MetaQuery<T> metaQuery, InfoForWiring wiring) {
		CommonTree expression = node.getASTNode();
		int type = expression.getType();
		log.debug("where type:" + expression.getType());
		switch (type) {
		case NoSqlLexer.AND:
		case NoSqlLexer.OR:
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
				
				parseExpression(childNode, metaQuery, wiring);
				
			}
			break;
		case NoSqlLexer.EQ:
		case NoSqlLexer.NE:
		case NoSqlLexer.GT:
		case NoSqlLexer.LT:
		case NoSqlLexer.GE:
		case NoSqlLexer.LE:
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
				throw new IllegalArgumentException("Currently, each param in the where clause must be compared to an attribute.  bad query="+wiring.getQuery()+" bad piece="+node.getExpressionAsString());
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

	private static boolean hasTypeInfo(CommonTree node) {
		if(node.getType() == NoSqlLexer.ATTR_NAME || node.getType() == NoSqlLexer.DECIMAL
				|| node.getType() == NoSqlLexer.INT_VAL || node.getType() == NoSqlLexer.STR_VAL)
			return true;
		return false;
	}

	private static TypeInfo processSide(MetaQuery metaQuery, ExpressionNode node, InfoForWiring wiring, TypeInfo typeInfo) {
		if(node.getType() == NoSqlLexer.ATTR_NAME) {
			return processAttribute(metaQuery, node, wiring, typeInfo);
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
		String withoutQuotes = constant.substring(1, constant.length()-1);
		
		
		StorageTypeEnum ourType;
		if(node.getType() == NoSqlLexer.DECIMAL){
			ourType = StorageTypeEnum.DECIMAL;
			//FIXME 
			node.setState(Double.parseDouble(withoutQuotes)); 
		}
		else if(node.getType() == NoSqlLexer.STR_VAL){
			ourType = StorageTypeEnum.STRING;
			node.setState(withoutQuotes);
		}
		else if(node.getType() == NoSqlLexer.INT_VAL){
			ourType = StorageTypeEnum.INTEGER;
			node.setState(Integer.getInteger(withoutQuotes));
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
		Class type = info.getClassType();
		if(constantType == StorageTypeEnum.STRING && 
				(type.equals(String.class) || type.equals(Boolean.class)
						|| type.equals(Character.class)))
			return;
		else if(constantType == StorageTypeEnum.DECIMAL && 
				(type.equals(Double.class) || type.equals(Float.class) ))
			return;
		else if(constantType == StorageTypeEnum.INTEGER &&
				(type.equals(Long.class) || type.equals(Integer.class) 
						|| type.equals(Short.class) || type.equals(Byte.class)))
			return;
		else
			throw new IllegalArgumentException("Types do not match in namedquery="+wiring.getQuery());
	}
	

	private static TypeInfo processAttribute(MetaQuery metaQuery, 
			ExpressionNode attributeNode2, InfoForWiring wiring, TypeInfo otherSideType) {
		DboTableMeta metaClass;
		
		CommonTree attributeNode = attributeNode2.getASTNode();
		String attributeName = attributeNode.getText();
		String textInSql = attributeName;
		if (attributeNode.getChildCount() > 0) {
			String aliasEntity = attributeNode.getChild(0).getText();
			
			metaClass = wiring.getInfoFromAlias(aliasEntity);
			textInSql = aliasEntity+"."+attributeName;
			if(metaClass == null)
				throw new RuntimeException("query="+metaQuery+" failed to parse because in where clause attribute="
						+textInSql+" has an alias that does not exist in from clause");
		} else {
			metaClass = wiring.getNoAliasTable();
			if(metaClass == null)
				throw new RuntimeException("query="+metaQuery+" failed to parse because in where clause attribute="
						+textInSql+" has no alias and from clause only has tables with alias");
		}
		
		//At this point, we have looked up the metaClass associated with the alias
		DboColumnMeta attributeField = metaClass.getColumnMeta(attributeName);
		if (attributeField == null) {
			//okay, there is no column found, but maybe the column name for the id matches(id is a special case)
			attributeField = metaClass.getIdColumnMeta();
			if(!attributeField.getColumnName().equals(attributeName))
				throw new IllegalArgumentException("There is no " + attributeName + " exists for class " + metaClass);
		}
		
		String colName = attributeField.getColumnName();
		if(!attributeField.isIndexed())
			throw new IllegalArgumentException("You cannot have '"+textInSql+"' in your sql query since "+attributeName+" has no @Index annotation on the field in the entity");
		
		StateAttribute attr = new StateAttribute(metaClass.getColumnFamily(), attributeField); 
		attributeNode2.setState(attr);
		
		TypeInfo typeInfo = new TypeInfo(attributeField);
		
		if(otherSideType == null)
			return typeInfo;
		
		if(otherSideType.getConstantType() != null) {
			validateTypes(wiring, otherSideType.getConstantType(), typeInfo);
		} else {
			Class classType = otherSideType.getColumnInfo().getClassType();
			if(!classType.equals(attributeField.getClassType()))
				throw new IllegalArgumentException("Types are not the same for query="+wiring.getQuery()+" types="+classType+" and "+attributeField.getClassType());
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
