package com.andreikubar.alfresco.migration.export;

import org.alfresco.service.cmr.repository.NodeRef;
import org.alfresco.service.namespace.QName;

public class ExportNode {
    public NodeRef nodeRef;
    public String fullPath;
    public boolean isFolder;
    public boolean isFile;
    public QName nodeType;
    public String name;
}
