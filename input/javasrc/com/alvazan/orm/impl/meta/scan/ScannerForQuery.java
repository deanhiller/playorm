package com.alvazan.orm.impl.meta.scan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Provider;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.anno.NoSqlQueries;
import com.alvazan.orm.api.base.anno.NoSqlQuery;
import com.alvazan.orm.api.spi.index.IndexReaderWriter;
import com.alvazan.orm.api.spi.index.SpiMetaQuery;
import com.alvazan.orm.impl.meta.data.MetaClass;
import com.alvazan.orm.impl.meta.data.MetaInfo;
import com.alvazan.orm.impl.meta.data.MetaQuery;
import com.alvazan.orm.impl.meta.data.MetaQueryClassInfo;
import com.alvazan.orm.impl.meta.data.MetaQueryFieldInfo;
import com.alvazan.orm.parser.antlr.NoSqlLexer;
import com.alvazan.orm.parser.antlr.NoSqlParser;

public class ScannerForQuery {

	private static final Logger log = LoggerFactory
			.getLogger(ScannerForQuery.class);
	@Inject
	private IndexReaderWriter indexes;
	@Inject
	private MetaInfo metaInfo;
	
	@SuppressWarnings("rawtypes")
	@Inject
	private Provider<MetaQuery> metaQueryFactory;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void setupQueryStuff(MetaClass classMeta) {
		Class<?> clazz = classMeta.getMetaClass();
		NoSqlQuery annotation = clazz.getAnnotation(NoSqlQuery.class);
		NoSqlQueries annotation2 = clazz.getAnnotation(NoSqlQueries.class);
		List<NoSqlQuery> theQueries = new ArrayList<NoSqlQuery>();
		if(annotation2 != null) {
			NoSqlQuery[] queries = annotation2.value();
			List<NoSqlQuery> asList = Arrays.asList(queries);
			theQueries.addAll(asList);
		}
		if(annotation != null)
			theQueries.add(annotation);

		log.info("Parsing queries for entity="+classMeta.getMetaClass());
		for(NoSqlQuery query : theQueries) {
			log.info("parsing query="+query.name()+" query="+query.query());
			MetaQuery metaQuery = createQueryAndAdd(classMeta, query);
			classMeta.addQuery(query.name(), metaQuery);
		}
	}
	
	@SuppressWarnings({ "rawtypes" })
	private MetaQuery createQueryAndAdd(MetaClass classMeta, NoSqlQuery query) {
		try {
			// parse and setup this query once here to be used by ALL of the
			// SpiIndexQuery objects.
			// NOTE: This is meta data to be re-used by all threads and all
			// instances of query objects only!!!!

			// We must walk the tree allowing 2 visitors to see it.
			// The first visitor would be ourselves maybe? to get all parameter info
			// The second visitor is the SPI Index so it can create it's "prototype"
			// query (prototype pattern)
			MetaQuery metaQuery = newsetupByVisitingTree(classMeta, query.query());

			return metaQuery;
		} catch(RuntimeException e) {
			throw new RuntimeException("Named query="+query.name()+" on class="
					+classMeta.getMetaClass()+" failed to parse.  query=\""+query.query()
					+"\"  See chained exception for cause", e);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> MetaQuery<T> newsetupByVisitingTree(MetaQueryClassInfo metaClass,
			String query) {
		CommonTree theTree = parseTree(query);
		MetaQuery<T> visitor1 = metaQueryFactory.get();
		SpiMetaQuery visitor2 = indexes.createQueryFactory();
		
		visitor1.initialize(metaClass, query, visitor2);

		InfoForWiring wiring = new InfoForWiring();
		
		// VISITOR PATTERN(if you don't know that pattern, google it!!!)
		// Normally, the visitor is injected INTO theTree variable itself but I
		// am too lazy to
		// figure out the code generation of antLR to do that plus that might
		// not be as flexible as this
		// anyways since more people can make changes without the grammar
		// knowledge this way
		walkTheTree(theTree, visitor1, visitor2, wiring);

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
			SpiMetaQuery spiMetaQuery, InfoForWiring wiring) {
		int type = tree.getType();
		switch (type) {
		case NoSqlLexer.FROM_CLAUSE:
			parseFromClause(tree, metaQuery, spiMetaQuery, wiring);
			break;
		case NoSqlLexer.SELECT_CLAUSE:
			parseSelectClause(tree, metaQuery, spiMetaQuery, wiring);
			break;
		case NoSqlLexer.WHERE:
			//We should try to get rid of the where token in the grammar so we don't need 
			//this line of code here....
			CommonTree expression = (CommonTree)tree.getChildren().get(0);
			parseExpression(expression, metaQuery, spiMetaQuery, wiring);
			//NOTE: We were going to call methods on the factory visitor during the tree walk of the
			//expression, but that makes for a difficult spi to implement for anyone so instead we will
			//just give the spi implementer the full expression after the WHERE clause in AST tree form
			//so he can create his own visitor pattern in his code
			//factory.
			spiMetaQuery.formQueryFromAstTree(expression);
			break;

		case 0: // nil
			List<CommonTree> childrenList = tree.getChildren();
			for (CommonTree child : childrenList) {
				walkTheTree(child, metaQuery, spiMetaQuery, wiring);
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
			MetaQuery<T> metaQuery, SpiMetaQuery factory, InfoForWiring wiring) {
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

	@SuppressWarnings("rawtypes")
	private <T> void loadTableIntoWiringInfo(MetaQuery<T> metaQuery,
			InfoForWiring wiring, CommonTree tableNode) {
		// What should we add to metaQuery here
		// AND later when we do joins, we need to tell the factory
		// here as well
		String tableName = tableNode.getText();
		List<MetaClass> list = metaInfo.findBySimpleName(tableName);
		
		MetaQueryClassInfo metaClass = null;
		
		if(tableName.equals("TABLE")) {
			metaClass = metaQuery.getMetaClass();
		} else if(tableName.contains(".")) {
			Class<?> clazz = findClass(tableName);
			metaClass = metaInfo.getMetaClass(clazz);
		} else if(list != null) {
			if(list.size() > 1) 
				throw new IllegalArgumentException("There are too many classes named="+tableName+"  Use fully qualified name instead.  query="+metaQuery);
			metaClass = list.get(0);
		} else
			throw new IllegalArgumentException("Query="+metaQuery+" failed to parse.  entity="+tableName+" cannot be found");
			
		if(tableNode.getChildren().size() == 0) {
			if(wiring.getNoAliasTable() != null)
				throw new IllegalArgumentException("Query="+metaQuery+" has two tables with no alias.  This is not allowed");
			wiring.setNoAliasTable(metaClass);
		} else {
			CommonTree aliasNode = (CommonTree) tableNode.getChildren().get(0);
			String alias = aliasNode.getText();
			wiring.put(alias, metaClass);
		}
	}

	@SuppressWarnings("rawtypes")
	private Class findClass(String name) {
		try {
			return Class.forName(name);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
	@SuppressWarnings("unchecked")
	private static <T> void parseExpression(CommonTree expression,
			MetaQuery<T> metaQuery, SpiMetaQuery factory, InfoForWiring wiring) {
		int type = expression.getType();
		log.info("where type:" + expression.getType());
		switch (type) {
		case NoSqlLexer.AND:
		case NoSqlLexer.OR:
			// no need to add attributes here anymore as we are using
			// visitor pattern
			List<CommonTree> children = expression.getChildren();
			for(CommonTree child : children)
				parseExpression(child, metaQuery, factory, wiring);
			break;
		case NoSqlLexer.EQ:
		case NoSqlLexer.NE:
		case NoSqlLexer.GT:
		case NoSqlLexer.LT:
		case NoSqlLexer.GE:
		case NoSqlLexer.LE:
			//The right side could be value/constant or variable or true or false, or decimal, etc. etc.
			CommonTree rightSide = (CommonTree) expression.getChild(0);
			CommonTree leftSide = (CommonTree) expression.getChild(1);
			
			//I think we only care about mapping parameters to the attribute meta data here....????
			if(rightSide.getType() == NoSqlLexer.ATTR_NAME && leftSide.getType() == NoSqlLexer.PARAMETER_NAME) {
				process(metaQuery, rightSide, leftSide, wiring);
			} else if(rightSide.getType() == NoSqlLexer.PARAMETER_NAME && leftSide.getType() == NoSqlLexer.ATTR_NAME) {
				process(metaQuery, leftSide, rightSide, wiring);
			}
			
			break;
		default:
			break;
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static void process(MetaQuery metaQuery, CommonTree attributeNode, CommonTree parameterNode, InfoForWiring wiring) {
		MetaQueryClassInfo metaClass;
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
		MetaQueryFieldInfo attributeField = metaClass.getMetaField(attributeName);
		if (attributeField == null && !metaClass.getIdFieldName().equals(attributeName)) {
			throw new IllegalArgumentException("There is no " + attributeName + " exists for class " + metaClass);
		}

		String parameter = parameterNode.getText();

		metaQuery.getParameterFieldMap().put(parameter, attributeField);		
	}

}
