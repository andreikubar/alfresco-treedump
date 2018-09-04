package com.andreikubar.alfresco.migration.export;

import org.alfresco.service.cmr.repository.NodeRef;

public class NodeRefExt {
    public NodeRef nodeRef;
    public String fullPath;

    public NodeRefExt(NodeRef nodeRef, String fullPath) {
        this.nodeRef = nodeRef;
        this.fullPath = fullPath;
    }
}
