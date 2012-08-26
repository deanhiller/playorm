package com.alvazan.orm.layer5.nosql.cache;

import javax.inject.Inject;
import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.exc.ParseException;
import com.alvazan.orm.api.spi3.meta.DboDatabaseMeta;
import com.alvazan.orm.api.spi3.meta.MetaQuery;
import com.alvazan.orm.layer5.indexing.ExpressionNode;
import com.alvazan.orm.layer5.indexing.SpiMetaQueryImpl;
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
	@Inject
	private SqlScanner compiler;
	
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
		MetaQuery visitor1 = metaQueryFactory.get();
		SpiMetaQueryImpl spiMetaQuery = factory.get(); 
		visitor1.initialize(query, spiMetaQuery);

		InfoForWiring wiring = new InfoForWiring(query, targetTable, (NoSqlEntityManager) mgr);
		MetaFacade facade = new MetaFacadeImpl((NoSqlEntityManager)mgr);
		
		ExpressionNode newTree = compiler.compileSql(query, wiring, facade);
		
		spiMetaQuery.setASTTree(newTree, wiring.getFirstTable());
		visitor1.setParameterFieldMap(wiring.getParameterFieldMap());
		visitor1.setTargetTable(wiring.getMetaQueryTargetTable());
		
		return visitor1;
	}
	


}
