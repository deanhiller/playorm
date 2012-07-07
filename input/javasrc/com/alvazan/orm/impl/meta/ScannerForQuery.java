package com.alvazan.orm.impl.meta;

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

import com.alvazan.orm.api.anno.NoSqlQueries;
import com.alvazan.orm.api.anno.NoSqlQuery;
import com.alvazan.orm.layer3.spi.index.IndexReaderWriter;
import com.alvazan.orm.layer3.spi.index.SpiMetaQuery;
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
		SpiMetaQuery<T> visitor2 = indexes.createQueryFactory();
		
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
			SpiMetaQuery<T> spiMetaQuery, InfoForWiring wiring) {
		int type = tree.getType();
		switch (type) {
		case NoSqlLexer.SELECT_CLAUSE:
			parseSelectClause(tree, metaQuery, spiMetaQuery);
			break;
		case NoSqlLexer.FROM_CLAUSE:
			parseFromClause(tree, metaQuery, spiMetaQuery, wiring);
			break;
		case NoSqlLexer.WHERE:
			//We should try to get rid of the where token in the grammar so we don't need 
			//this line of code here....
			CommonTree expression = (CommonTree)tree.getChildren().get(0);
			parseExpression(expression, metaQuery, spiMetaQuery);
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
			MetaQuery<T> metaQuery, SpiMetaQuery<T> factory) {

		List<CommonTree> childrenList = tree.getChildren();
		if (childrenList != null && childrenList.size() > 0) {
			for (CommonTree child : childrenList) {
				switch (child.getType()) {
				case NoSqlLexer.RESULT:
					parseResult(child, metaQuery, factory);
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
			MetaQuery<T> metaQuery, SpiMetaQuery<T> factory) {
		MetaQueryClassInfo metaClass = metaQuery.getMetaClass();
		List<CommonTree> childrenList = tree.getChildren();
		if (childrenList == null)
			return;
			
		for (CommonTree child : childrenList) {
			switch (child.getType()) {
			case NoSqlLexer.STAR:
				metaQuery.getProjectionFields().addAll(metaClass.getMetaFields());
				break;
			case NoSqlLexer.ATTR_NAME:
				String attributeName = child.getText();
				MetaQueryFieldInfo projectionField = metaClass.getMetaField(attributeName);
				if (projectionField == null)
					throw new IllegalArgumentException("There is no "
							+ attributeName + " exists for class "
							+ metaClass);
				metaQuery.getProjectionFields().add(projectionField);

				break;
			case NoSqlLexer.ALIAS:
				break;

			default:
				break;
			}
		}

	}

	@SuppressWarnings("unchecked")
	private <T> void parseFromClause(CommonTree tree,
			MetaQuery<T> metaQuery, SpiMetaQuery<T> factory, InfoForWiring wiring) {
		List<CommonTree> childrenList = tree.getChildren();
		if (childrenList == null)
			return;
		
		for (CommonTree child : childrenList) {
			int type = child.getType();
			switch (type) {
			case NoSqlLexer.TABLE_NAME:
				// What should we add to metaQuery here
				// AND later when we do joins, we need to tell the factory
				// here as well
				String tableName = child.getText();
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
					
				

				break;
			default:
				break;
			}
		}
	}

	private Class findClass(String name) {
		try {
			return Class.forName(name);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
	@SuppressWarnings("unchecked")
	private static <T> void parseExpression(CommonTree expression,
			MetaQuery<T> metaQuery, SpiMetaQuery<T> factory) {
		MetaQueryClassInfo metaClass = metaQuery.getMetaClass();

		int type = expression.getType();
		log.info("where type:" + expression.getType());
		switch (type) {
		case NoSqlLexer.AND:
		case NoSqlLexer.OR:
			// no need to add attributes here anymore as we are using
			// visitor pattern
			List<CommonTree> children = expression.getChildren();
			for(CommonTree child : children)
				parseExpression(child, metaQuery, factory);
			
			break;
		case NoSqlLexer.EQ:
		case NoSqlLexer.NE:
		case NoSqlLexer.GT:
		case NoSqlLexer.LT:
		case NoSqlLexer.GE:
		case NoSqlLexer.LE:
			int start = 0;
			String aliasEntity;
			if (expression.getChild(0).getType() == NoSqlLexer.ALIAS) {
				aliasEntity = expression.getChild(0).getText();
				start = 1;
			}
			String attributeName = expression.getChild(start).getText();

			MetaQueryFieldInfo attributeField = metaClass.getMetaField(attributeName);
			if (attributeField == null && !metaClass.getIdFieldName().equals(attributeName)) {
				throw new IllegalArgumentException("There is no "
						+ attributeName + " exists for class " + metaClass);
			}

			String parameter = expression.getChild(start + 1).getText();

			metaQuery.getParameterFieldMap().put(parameter, attributeField);
			
			break;
		default:
			break;
		}
	}

}
