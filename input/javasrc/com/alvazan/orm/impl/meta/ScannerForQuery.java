package com.alvazan.orm.impl.meta;

import java.util.List;

import javax.inject.Inject;
import javax.inject.Provider;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.anno.NoSqlQuery;
import com.alvazan.orm.layer3.spi.index.IndexReaderWriter;
import com.alvazan.orm.layer3.spi.index.SpiIndexQueryFactory;
import com.alvazan.orm.parser.antlr.NoSqlLexer;
import com.alvazan.orm.parser.antlr.NoSqlParser;

public class ScannerForQuery {

	private static final Logger log = LoggerFactory
			.getLogger(ScannerForQuery.class);
	@Inject
	private IndexReaderWriter indexes;

	@SuppressWarnings("rawtypes")
	@Inject
	private Provider<MetaQuery> metaQueryFactory;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public MetaQuery createQueryAndAdd(MetaClass classMeta, NoSqlQuery query) {
		try {
			// 	This is a bit messed up and need to clean up one more step
			MetaQuery metaQuery = setup(classMeta, query.query());

			return metaQuery;
		} catch(RuntimeException e) {
			throw new RuntimeException("Named query="+query.name()+" on class="
					+classMeta.getMetaClass()+" failed to parse.  query=\""+query.query()
					+"\"  See chained exception for cause", e);
		}
	}

	public <T> MetaQuery<T> setup(MetaQueryClassInfo metaClass, String query) {
		// parse and setup this query once here to be used by ALL of the
		// SpiIndexQuery objects.
		// NOTE: This is meta data to be re-used by all threads and all
		// instances of query objects only!!!!

		// We must walk the tree allowing 2 visitors to see it.
		// The first visitor would be ourselves maybe? to get all parameter info
		// The second visitor is the SPI Index so it can create it's "prototype"
		// query (prototype pattern)
		return newsetupByVisitingTree(metaClass, query);
	}

	private <T> MetaQuery<T> newsetupByVisitingTree(MetaQueryClassInfo metaClass,
			String query) {
		CommonTree theTree = parseTree(query);
		MetaQuery<T> visitor1 = metaQueryFactory.get();
		SpiIndexQueryFactory<T> visitor2 = indexes.createQueryFactory();
		
		visitor1.initialize(metaClass, query, visitor2);

		// VISITOR PATTERN(if you don't know that pattern, google it!!!)
		// Normally, the visitor is injected INTO theTree variable itself but I
		// am too lazy to
		// figure out the code generation of antLR to do that plus that might
		// not be as flexible as this
		// anyways since more people can make changes without the grammar
		// knowledge this way
		walkTheTree(theTree, visitor1, visitor2);

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
	
	private <T> void walkTheTree(CommonTree tree, MetaQuery<T> metaQuery,
			SpiIndexQueryFactory<T> factory) {
		int type = tree.getType();
		switch (type) {
		case NoSqlLexer.SELECT_CLAUSE:
			parseSelectClause(tree, metaQuery, factory);
			break;
		case NoSqlLexer.FROM_CLAUSE:
			parseFromClause(tree, metaQuery, factory);
			break;
		case NoSqlLexer.WHERE:
			parseExpression(tree, metaQuery, factory);
			break;

		case 0: // nil
			List<CommonTree> childrenList = tree.getChildren();
			for (CommonTree child : childrenList) {
				walkTheTree(child, metaQuery, factory);
			}
			break;
		default:
			break;
		}
	}

	@SuppressWarnings("unchecked")
	private static <T> void parseSelectClause(CommonTree tree,
			MetaQuery<T> metaQuery, SpiIndexQueryFactory<T> factory) {

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
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static <T> void parseResult(CommonTree tree,
			MetaQuery<T> metaQuery, SpiIndexQueryFactory<T> factory) {
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
	private static <T> void parseFromClause(CommonTree tree,
			MetaQuery<T> metaQuery, SpiIndexQueryFactory<T> factory) {
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

				break;
			default:
				break;
			}
		}
	}

	@SuppressWarnings("unchecked")
	private static <T> void parseExpression(CommonTree tree,
			MetaQuery<T> metaQuery, SpiIndexQueryFactory<T> factory) {
		MetaQueryClassInfo metaClass = metaQuery.getMetaClass();
		List<CommonTree> childrenList = tree.getChildren();
		if (childrenList == null)
			return;

		for (CommonTree child : childrenList) {
			int type = child.getType();
			log.info("where type:" + child.getType());
			switch (type) {
			case NoSqlLexer.AND:
			case NoSqlLexer.OR:
				// no need to add attributes here anymore as we are using
				// visitor pattern
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
				if (child.getChild(0).getType() == NoSqlLexer.ALIAS) {
					aliasEntity = child.getChild(0).getText();
					start = 1;
				}
				String attributeName = child.getChild(start).getText();

				MetaQueryFieldInfo attributeField = metaClass.getMetaField(attributeName);
				if (attributeField == null && !metaClass.getIdFieldName().equals(attributeName)) {
					throw new IllegalArgumentException("There is no "
							+ attributeName + " exists for class " + metaClass);
				}

				String parameter = child.getChild(start + 1).getText();

				metaQuery.getParameterFieldMap().put(parameter, attributeField);
				
				break;
			default:
				break;
			}
		}
	}

}
