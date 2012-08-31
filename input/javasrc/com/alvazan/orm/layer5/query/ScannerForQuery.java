package com.alvazan.orm.layer5.query;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.exc.ParseException;
import com.alvazan.orm.api.z5api.MetaQuery;
import com.alvazan.orm.api.z5api.QueryParser;
import com.alvazan.orm.api.z8spi.NoSqlRawSession;
import com.alvazan.orm.api.z8spi.meta.DboDatabaseMeta;
import com.alvazan.orm.parser.antlr.ExpressionNode;
import com.alvazan.orm.parser.antlr.InfoForWiring;
import com.alvazan.orm.parser.antlr.MetaFacade;
import com.alvazan.orm.parser.antlr.ParseQueryException;
import com.alvazan.orm.parser.antlr.ScannerSql;

@SuppressWarnings("rawtypes")
public class ScannerForQuery implements QueryParser {

	@Inject
	@Named("logger")
	private NoSqlRawSession rawSession;
	@Inject
	private Provider<SpiMetaQueryImpl> factory;
	@Inject
	private DboDatabaseMeta metaInfo;
	@Inject
	private Provider<MetaQueryImpl> metaQueryFactory;
	@Inject
	private ScannerSql compiler;
	
	@Override
	public MetaQuery parseQueryForAdHoc(String query, Object mgr) {		
		MetaQuery metaQuery = newsetupByVisitingTree(query, null, mgr, "Query="+query+". ");
		return metaQuery;
	}

	@Override
	public MetaQuery parseQueryForOrm(String query, String targetTable, String errorMsg) {
		return newsetupByVisitingTree(query, targetTable, null, errorMsg);
	}

	private MetaQuery newsetupByVisitingTree(String query, String targetTable, Object mgr, String errorMsg) {
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
		MetaQueryImpl visitor1 = metaQueryFactory.get();
		SpiMetaQueryImpl spiMetaQuery = factory.get(); 
		visitor1.initialize(query, spiMetaQuery);

		InfoForWiring wiring = new InfoForWiring(query, targetTable);
		MetaFacade facade = new MetaFacadeImpl((NoSqlEntityManager)mgr, metaInfo);
		ExpressionNode newTree = compiler.compileSql(query, wiring, facade);
		
		spiMetaQuery.setASTTree(newTree, wiring.getFirstTable());
		visitor1.setParameterFieldMap(wiring.getParameterFieldMap());
		visitor1.setTargetTable(wiring.getMetaQueryTargetTable());
		
		return visitor1;
	}
	
	@Override
	public void close() {
		rawSession.close();
	}

}
