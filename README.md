# Dumps the file tree starting from a list of given NodeId's into a csv
Tested on Alfresco 4.2.8, 5.2.3

## CSV Format:
level,parentNodeId,childNodeId,NodeName,NodeType,isFolder,FullPath

# Installation

- Deploy the JAR into WEB-INF/lib
- Copy log4j.properties to tomcat/shared/classes/alfresco/extension

# Usage
## Start the dump process
http://<alfresco_url>/alfresco/service/treedump.html

## Cancel the running dump process
http://<alfresco_url>/alfresco/service/treedump/shutdown.html