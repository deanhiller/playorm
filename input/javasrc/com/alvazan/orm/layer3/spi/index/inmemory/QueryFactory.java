package com.alvazan.orm.layer3.spi.index.inmemory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.spi.index.SpiMetaQuery;
import com.alvazan.orm.api.spi.index.SpiQueryAdapter;
import com.alvazan.orm.impl.meta.query.MetaFieldDbo;

public class QueryFactory implements SpiMetaQuery {

	private static final Logger log = LoggerFactory.getLogger(QueryFactory.class);
	@Inject
	private Provider<SpiIndexQueryImpl> factory;
	private Indice indice;
	
	private QueryNode root;
	
	//pointer is used for setting up the tree;
	private QueryNode pointer;
	
	
	
	public void init(Indice indice) {
		this.indice = indice;
	}
	
	@Override
	public SpiQueryAdapter createQueryInstanceFromQuery(String indexName) {
		log.info("creating query for index="+indexName);
		SpiIndexQueryImpl indexQuery = factory.get();
		IndexItems index = indice.find(indexName);
		indexQuery.setup(index, this);
		return indexQuery;
	}

	
	
	@Override
	public void onHyphen(int type) {
		if(pointer==null){
			root = new HyphenNode(null,type);
			pointer = root;
			return;
		}
		if(((HyphenNode)pointer).getLeftNode()==null){
			((HyphenNode)pointer).setLeftNode(new HyphenNode(pointer,type));
			pointer = ((HyphenNode)pointer).getLeftNode();
		}else{
			((HyphenNode)pointer).setRightNode(
					new HyphenNode(pointer,type));
			pointer = ((HyphenNode)pointer).getRightNode();
		}
	}

	@Override
	public void onComparator(String parameter, MetaFieldDbo attributeField,
			int type) {
		if(pointer ==null){
			root = new LeafNode(null,attributeField,parameter,type);
			pointer = root;
			List<LeafNode> nodes = new ArrayList<LeafNode>();
			nodes.add((LeafNode)root);
//			this.paraNameToLeafNodes.put(parameter,nodes);
			return;
		}
		LeafNode node = new LeafNode(pointer,attributeField,parameter,type); 
		if(((HyphenNode)pointer).getLeftNode()==null){
			((HyphenNode)pointer).setLeftNode(node);
			
		}else{
			((HyphenNode)pointer).setRightNode(node);
			pointer = pointer.getParentNode();
		}
//		List<LeafNode> nodes = this.paraNameToLeafNodes.get(parameter);
//		if(nodes==null){
//			nodes= new ArrayList<LeafNode>();
//			this.paraNameToLeafNodes.put(parameter, nodes);
//		}
//		nodes.add(node);
	}

	@Override
	public Query getQuery(
			Map<String, Object> parameterValues) {
		return root.getQuery(parameterValues);
	}

	
}
