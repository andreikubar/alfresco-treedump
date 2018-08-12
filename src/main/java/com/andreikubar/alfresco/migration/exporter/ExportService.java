package com.andreikubar.alfresco.migration.exporter;

import com.andreikubar.alfresco.migration.exporter.model.ExportNode;
import com.andreikubar.alfresco.migration.exporter.proto.ExportProtos;
import org.alfresco.service.ServiceRegistry;
import org.alfresco.service.cmr.repository.NodeRef;
import org.alfresco.service.cmr.repository.NodeService;
import org.alfresco.service.cmr.repository.StoreRef;

public class ExportService {

    private ServiceRegistry serviceRegistry;
    private NodeService nodeService;
    private ExportNodeBuilder exportNodeBuilder;

    public ExportService(ServiceRegistry serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
        this.nodeService = serviceRegistry.getNodeService();
    }

    public void setExportNodeBuilder(ExportNodeBuilder exportNodeBuilder) {
        this.exportNodeBuilder = exportNodeBuilder;
    }

/*
    public byte[] dumpMetadataToProtobuf(String parentPath, String name){
        to be used for getting metadata of a parent directory, in case it will
        not exist at node import time
    }
*/

    public byte[] dumpMetadataToProtobuf(String nodeId) {
        NodeRef nodeRef = new NodeRef(StoreRef.STORE_REF_WORKSPACE_SPACESSTORE, nodeId);
        ExportNode exportNode = exportNodeBuilder.constructExportNode(nodeRef);
        ExportProtos.ExportBatch exportBatch = buildExportBatch(exportNode);
        return exportBatch.toByteArray();
    }

    private ExportProtos.ExportBatch buildExportBatch(ExportNode exportNode) {
        ExportProtos.ExportBatch.Builder exportBatch = ExportProtos.ExportBatch.newBuilder();
        ExportProtos.AlfrescoNode.Builder alfrescoNode = ExportProtos.AlfrescoNode.newBuilder();
        alfrescoNode.setName(exportNode.name)
                .setFullPath(exportNode.fullPath)
                .setIsFolder(exportNode.isFolder)
                .setType(exportNode.typePrefixed);
        if (exportNode.url != null) {
            alfrescoNode.setUrl(exportNode.url);
        }

        exportBatch.addNodes(alfrescoNode.build());
        return exportBatch.build();
    }
}
