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

import com.alvazan.orm.api.spi.index.ExpressionNode;
import com.alvazan.orm.api.spi.index.IndexReaderWriter;
import com.alvazan.orm.api.spi.index.SpiMetaQuery;
import com.alvazan.orm.api.spi.index.StateAttribute;
import com.alvazan.orm.api.spi.layer2.MetaColumnDbo;
import com.alvazan.orm.api.spi.layer2.MetaDatabase;
import com.alvazan.orm.api.spi.layer2.MetaQuery;
import com.alvazan.orm.api.spi.layer2.MetaTableDbo;
import com.alvazan.orm.parser.antlr.NoSqlLexer;
import com.alvazan.orm.parser.antlr.NoSqlParser;

@SuppressWarnings("rawtypes")
public class ScannerForQuery {

	private static final Logger log = LoggerFactory
			.getLogger(ScannerForQuery.class);
	@Inject
	private IndexReaderWriter indexes;
	@Inject
	private MetaDatabase metaInfo;
	
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
		SpiMetaQuery visitor2 = indexes.createQueryFactory();
		
		visitor1.initialize(query, visitor2);

		InfoForWiring wiring = new InfoForWiring();
		
		// VISITOR PATTERN(if you don't know that pattern, google it!!!)
		// Normally, the visitor is injected INTO theTree variable itself but I
		// am too lazy to
		// figure out the code generation of antLR to do that plus that might
		// not be as flexible as this
		// anyways since more people can make changes without the grammar
		// knowledge this way
		walkTheTree(theTree, visitor1, visitor2, wiring, targetTable);

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
	private <T> void walkTheTree(CommonTree tree, MetaQuery<T> metaQuery,
			SpiMetaQuery spiMetaQuery, InfoForWiring wiring, String targetTable) {
		int type = tree.getType();
		switch (type) {
		case NoSqlLexer.FROM_CLAUSE:
			parseFromClause(tree, metaQuery, spiMetaQuery, wiring, targetTable);
			break;
		case NoSqlLexer.SELECT_CLAUSE:
			parseSelectClause(tree, metaQuery, spiMetaQuery, wiring);
			break;
		case NoSqlLexer.WHERE:
			//We should try to get rid of the where token in the grammar so we don't need 
			//this line of code here....
			CommonTree expression = (CommonTree)tree.getChildren().get(0);
			ExpressionNode node = new ExpressionNode(expression);
			parseExpression(node, metaQuery, spiMetaQuery, wiring);
			spiMetaQuery.setASTTree(node);
			break;

		case 0: // nil
			List<CommonTree> childrenList = tree.getChildren();
			for (CommonTree child : childrenList) {
				walkTheTree(child, metaQuery, spiMetaQuery, wiring, targetTable);
			}
			break;
		default:
			break;
		}
	}

	@SuppressWarnings("unchecked")
	private static <T> void parseSelectClause(CommonTree tree,
			MetaQuery<T> metaQuery, SpiMetaQuery factory, InfoForWiring wiring) {

		List<CommonTree> childrenList = tree.getChildren();
		if (childrenList != null && childrenList.size() > 0) {
			for (CommonTree child : childrenList) {
				switch (child.getType()) {
				case NoSqlLexer.RESULT:
					parseResult(child, metaQuery, factory, wiring);
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
			MetaQuery<T> metaQuery, SpiMetaQuery factory, InfoForWiring wiring) {
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
			MetaQuery<T> metaQuery, SpiMetaQuery factory, InfoForWiring wiring, String targetTable) {
		List<CommonTree> childrenList = tree.getChildren();
		if (childrenList == null)
			return;
		
		for (CommonTree child : childrenList) {
			int type = child.getType();
			switch (type) {
			case NoSqlLexer.TABLE_NAME:
				loadTableIntoWiringInfo(metaQuery, wiring, child, targetTable);
				break;
			default:
				break;
			}
		}
	}

	private <T> void loadTableIntoWiringInfo(MetaQuery<T> metaQuery,
			InfoForWiring wiring, CommonTree tableNode, String targetTable) {
		// What should we add to metaQuery here
		// AND later when we do joins, we need to tell the factory
		// here as well
		String tableName = tableNode.getText();
		MetaTableDbo metaClass = metaInfo.getMeta(tableName);
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
			MetaQuery<T> metaQuery, SpiMetaQuery spiMetaQuery, InfoForWiring wiring) {
		CommonTree expression = node.getASTNode();
		int type = expression.getType();
		log.debug("where type:" + expression.getType());
		switch (type) {
		case NoSqlLexer.AND:
		case NoSqlLexer.OR:
			spiMetaQuery.onHyphen(type);
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
				
				parseExpression(childNode, metaQuery, spiMetaQuery, wiring);
				
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
			ExpressionNode left  = new ExpressionNode(leftSide);
			ExpressionNode right = new ExpressionNode(rightSide);
			node.setLeftChild(left);
			node.setRightChild(right);
			
			//I think we only care about mapping parameters to the attribute meta data here....????
			if(rightSide.getType() == NoSqlLexer.ATTR_NAME && leftSide.getType() == NoSqlLexer.PARAMETER_NAME) {
				process(metaQuery,spiMetaQuery, right, left, wiring,type);
			} else if(rightSide.getType() == NoSqlLexer.PARAMETER_NAME && leftSide.getType() == NoSqlLexer.ATTR_NAME) {
				process(metaQuery,spiMetaQuery, left, right, wiring,type);
			} else
				throw new UnsupportedOperationException("we don't support these two types together at this point.  lefttype="+leftSide.getType()+" rightType="+rightSide.getType());
			
			break;
		default:
			break;
		}
	}

	@SuppressWarnings({ "unchecked" })
	//too many arguments
	private static void process(MetaQuery metaQuery, SpiMetaQuery spiMetaQuery,
			ExpressionNode attributeNode2, ExpressionNode parameterNode2,
			InfoForWiring wiring, int type) {
		MetaTableDbo metaClass;
		
		CommonTree attributeNode = attributeNode2.getASTNode();
		CommonTree parameterNode = parameterNode2.getASTNode();
		String attributeName = attributeNode.getText();
		if (attributeNode.getChildCount() > 0) {
			String aliasEntity = attributeNode.getChild(0).getText();
			
			metaClass = wiring.getInfoFromAlias(aliasEntity);
			String fullName = aliasEntity+"."+attributeName;
			if(metaClass == null)
				throw new RuntimeException("query="+metaQuery+" failed to parse because in where clause attribute="
						+fullName+" has an alias that does not exist in from clause");
		} else {
			metaClass = wiring.getNoAliasTable();
			if(metaClass == null)
				throw new RuntimeException("query="+metaQuery+" failed to parse because in where clause attribute="
						+attributeName+" has no alias and from clause only has tables with alias");
		}
		
		//At this point, we have looked up the metaClass associated with the alias
		MetaColumnDbo attributeField = metaClass.getMetaField(attributeName);
		if (attributeField == null) {
			throw new IllegalArgumentException("There is no " + attributeName + " exists for class " + metaClass);
		}
		
		String parameter = parameterNode.getText();

		metaQuery.getParameterFieldMap().put(parameter, attributeField);		
		spiMetaQuery.onComparator(parameter, attributeField.getColumnName(),type);
		
		StateAttribute attr = new StateAttribute(metaClass.getTableName(), attributeField.getColumnName()); 
		attributeNode2.setState(attr);
		parameterNode2.setState(parameter);
	}

}
