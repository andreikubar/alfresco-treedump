package com.andreikubar.alfresco.migration.export;

import org.alfresco.service.cmr.repository.NodeRef;
import org.alfresco.service.namespace.QName;

import java.io.Serializable;
import java.util.Map;

public class ExportNode {
    public NodeRef nodeRef;
    public String fullPath;
    public boolean isFolder;
    public boolean isFile;
    public QName nodeType;
    public String name;
    public Map<QName, Serializable> properties;
    public String contentUrl = "";
    public long contentBytes = 0;

    public ExportNode() {
    }

    public ExportNode(NodeRef nodeRef, String fullPath, boolean isFolder) {
        this.nodeRef = nodeRef;
        this.fullPath = fullPath;
        this.isFolder = isFolder;
    }

    public ExportNode(NodeRef nodeRef, String fullPath) {
        this.nodeRef = nodeRef;
        this.fullPath = fullPath;
    }
}
