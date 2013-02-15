package com.alvazan.orm.layer5.query;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import com.alvazan.orm.api.exc.ParseException;
import com.alvazan.orm.api.z5api.QueryParser;
import com.alvazan.orm.api.z5api.SpiMetaQuery;
import com.alvazan.orm.api.z8spi.MetaLoader;
import com.alvazan.orm.api.z8spi.NoSqlRawSession;
import com.alvazan.orm.api.z8spi.meta.DboDatabaseMeta;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;
import com.alvazan.orm.parser.antlr.ExpressionNode;
import com.alvazan.orm.parser.antlr.InfoForWiring;
import com.alvazan.orm.parser.antlr.MetaFacade;
import com.alvazan.orm.parser.antlr.ParseQueryException;
import com.alvazan.orm.parser.antlr.ScannerSql;

public class ScannerForQuery implements QueryParser {

	@Inject
	@Named("logger")
	private NoSqlRawSession rawSession;
	@Inject
	private Provider<SpiMetaQueryImpl> factory;
	@Inject
	private DboDatabaseMeta metaInfo;
	@Inject
	private ScannerSql compiler;
	
	@Override
	public SpiMetaQuery parseQueryForAdHoc(String query, MetaLoader mgr) {		
		SpiMetaQuery metaQuery = newsetupByVisitingTree(query, null, mgr, "Query="+query+". ");
		return metaQuery;
	}

	@Override
	public SpiMetaQuery parseQueryForOrm(String query, String targetTable, String errorMsg) {
		return newsetupByVisitingTree(query, targetTable, null, errorMsg);
	}

	private SpiMetaQuery newsetupByVisitingTree(String query, String targetTable, MetaLoader mgr, String errorMsg) {
		try {
			return newsetupByVisitingTreeImpl(query, targetTable, mgr, errorMsg);
		} catch(ParseQueryException e) {
			String msg = errorMsg+"  failed to parse.  Specific reason="+e.getMessage();
			throw new ParseException(msg, e);
		} catch(RuntimeException e) {
			throw new ParseException(errorMsg+" failed to compile.  See chained exception for cause", e);
		}
	}
	
	private SpiMetaQuery newsetupByVisitingTreeImpl(String query, String targetTable, MetaLoader mgr, String errorMsg) {
		SpiMetaQueryImpl spiMetaQuery = factory.get(); 

		InfoForWiring wiring = new InfoForWiring(query, targetTable);
		MetaFacade facade = new MetaFacadeImpl(mgr, metaInfo);
		ExpressionNode newTree = compiler.compileSql(query, wiring, facade);
		
		List<ViewInfo> allViews = wiring.getAllViews();
		List<ViewInfo> joinedViews = wiring.getJoinedViews();
		List<ViewInfo> notYetJoinedViews = new ArrayList<ViewInfo>();
		for(ViewInfo view : allViews) {
			if(!joinedViews.contains(view))
				notYetJoinedViews.add(view);
		}
		
		spiMetaQuery.setASTTree(newTree, joinedViews, notYetJoinedViews);
		spiMetaQuery.setQuery(query);
		spiMetaQuery.setParameterFieldMap(wiring.getParameterFieldMap());
		spiMetaQuery.setUpdateList(wiring.getUpdateList());
		spiMetaQuery.setQueryType(wiring.getQueryType());
		
		return spiMetaQuery;
	}
	
	@Override
	public void close() {
		rawSession.close();
	}

}
