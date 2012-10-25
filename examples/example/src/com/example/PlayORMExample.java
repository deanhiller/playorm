package com.example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.alvazan.orm.api.base.Bootstrap;
import com.alvazan.orm.api.base.DbTypeEnum;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.z3api.NoSqlTypedSession;
import com.alvazan.orm.api.z3api.QueryResult;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnToManyMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.TypedColumn;
import com.alvazan.orm.api.z8spi.meta.TypedRow;
import com.example.db.User;


 
public class PlayORMExample {
	private static NoSqlEntityManager mgr;
	
	public static void main(String[] args) {
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put(Bootstrap.AUTO_CREATE_KEY, "create");
		NoSqlEntityManagerFactory factory = Bootstrap.create(DbTypeEnum.IN_MEMORY, properties,null,null);
		mgr = factory.createEntityManager();
		createTestData();
		processSQL(args);
	}
	
	private static void processSQL(String[] args){
		StringBuilder builder = new StringBuilder();
		for(String s : args) {
		    builder.append(s);
		    builder.append(" ");
		}
		NoSqlTypedSession ntsession = mgr.getTypedSession();
        QueryResult result = ntsession.createQueryCursor(builder.toString(), 50);
        Cursor<List<TypedRow>> cursor = result.getAllViewsCursor();
        processBatch(cursor);
	}
	 /**
     * 
     * Print all the values inside the cursor 
     */
    private static void processBatch(Cursor<List<TypedRow>> rowsIter) {
        while(rowsIter.next()) {
            List<TypedRow> joinedRow = rowsIter.getCurrent();
            for(TypedRow r: joinedRow) {
                if (r!=null){            	
                    DboTableMeta meta = r.getView().getTableMeta();
                    DboColumnIdMeta idColumnMeta = meta.getIdColumnMeta();
                    String columnName = idColumnMeta.getColumnName();
                    System.out.println("RowKey:"+r.getRowKeyString()+" ("+columnName+")");
                    for(TypedColumn c : r.getColumnsAsColl()) {
                        DboColumnMeta colMeta = c.getColumnMeta();
                        if(colMeta != null) {
            				String fullName = c.getName();
            				String val = c.getValueAsString();
            				System.out.println("=> "+fullName+" = "+val);
            			} else {
            				String fullName = c.getNameAsString(byte[].class);
            				String val = c.getValueAsString(byte[].class);
            				System.out.println("=> "+fullName+" = "+ val);
            			}
                    }
                } 
            }

        }        
    }
    
	private static void createTestData() {
		User user1 = new User();
		user1.setName("TestName");
		user1.setAge(25);
		user1.setLastName("TestLastName");
		
		User user2 = new User();
		user2.setName("TestName2");
		user2.setAge(30);
		
		User user3 = new User();
		user3.setName("TestName3");
		user3.setLastName("TestLastName");
				
        mgr.put(user1);
        mgr.put(user2);
        mgr.put(user3);
        mgr.flush();
		
	}
}
