This folder is where jars go that 
1. are not needed during runtime
OR
2. are needed at runtime, but the jars are part of the runtime already and do not need to be delivered

For normal applications, jars in this directory are not delivered to output/jardist

For web applications, jars in this directory are not put in the war file
For osgi bundles, jars in this directory are not put in the osgi bundle

Examples of jars that belong in this directory are 
mocklib.jar, osgi's framework.jar, servlet.jar, etc. etc.