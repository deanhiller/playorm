/**
 * @COPYRIGHT (C) 2010 Hotel Reservation Service (HRS) Robert Ragge GmbH
 *
 * All rights reserved
 */
package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;

/**
 * @author Robert Stupp (last modified by $Author$)
 * @version $Revision$ $Date$
 */
@NoSqlDiscriminatorColumn("nestedC")
public class InheritedNestedC extends InheritedNestedBaseForC {
    private String helloWorld;

    public String getHelloWorld() {
        return helloWorld;
    }

    public void setHelloWorld(String helloWorld) {
        this.helloWorld = helloWorld;
    }
}
