package com.alvazan.orm.layer3.spi.index.inmemory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi3.index.ExpressionNode;
import com.alvazan.orm.api.spi3.index.SpiQueryAdapter;
import com.alvazan.orm.api.spi3.index.StateAttribute;
import com.alvazan.orm.api.spi3.index.ValAndType;
import com.alvazan.orm.parser.antlr.NoSqlLexer;

public class SpiIndexQueryImpl implements SpiQueryAdapter {

	private static final Logger log = LoggerFactory.getLogger(SpiIndexQueryImpl.class);
	
	private String indexName;
	private SpiMetaQueryImpl spiMeta;
	private NoSqlSession session;
	private Map<String, ValAndType> parameters = new HashMap<String, ValAndType>();
	
	public void setup(String indexName, SpiMetaQueryImpl spiMetaQueryImpl, NoSqlSession session) {
		this.indexName = indexName;
		this.spiMeta = spiMetaQueryImpl;
		this.session = session;
	}

	@Override
	public void setParameter(String parameterName, ValAndType valAndType) {
		parameters.put(parameterName, valAndType);
	}

	@Override
	public List getResultList() {
		ExpressionNode root = spiMeta.getASTTree();
		
		log.info("root="+root.getExpressionAsString());
		if(root.getType() == NoSqlLexer.EQ) {
			ExpressionNode child = root.getLeftChild();
			StateAttribute attr = (StateAttribute) child.getLeftChild().getState();
			String paramName = (String) child.getRightChild().getState();
			ValAndType val = parameters.get(paramName);
			if(val == null)
				throw new IllegalStateException("You did not call setParameter for parameter= ':"+paramName+"'");
			
			String table = attr.getTableName();
			
			
		} else
			throw new UnsupportedOperationException("not supported yet");
		
		//session.columnRangeScan(cf, indexKey, from, to, batchSize)
		return null;
	}

	
}
