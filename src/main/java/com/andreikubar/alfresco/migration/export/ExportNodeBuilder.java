package com.andreikubar.alfresco.migration.export;

import org.alfresco.model.ContentModel;
import org.alfresco.service.ServiceRegistry;
import org.alfresco.service.cmr.dictionary.DictionaryService;
import org.alfresco.service.cmr.repository.ChildAssociationRef;
import org.alfresco.service.cmr.repository.NodeService;
import org.alfresco.service.namespace.NamespaceService;
import org.alfresco.service.namespace.QName;


public class ExportNodeBuilder {
    private static final QName FOLDER_TYPE = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "folder");
    private static final QName FILE_TYPE = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "content");

    private ServiceRegistry serviceRegistry;
    private Boolean useCmName;

    public void setServiceRegistry(ServiceRegistry serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
    }

    public void setUseCmName(Boolean useCmName) {
        this.useCmName = useCmName;
    }

    public Boolean getUseCmName() {
        return useCmName;
    }

    public ExportNode constructExportNode(ChildAssociationRef childAssociationRef, String parentFullPath){
        NodeService nodeService = serviceRegistry.getNodeService();
        DictionaryService dictionaryService = serviceRegistry.getDictionaryService();

        if (useCmName == null) useCmName = true;

        ExportNode node = new ExportNode();
        node.name = useCmName ? (String) nodeService.getProperty(
                childAssociationRef.getChildRef(), ContentModel.PROP_NAME)
                : childAssociationRef.getQName().getLocalName();
        node.nodeRef = childAssociationRef.getChildRef();
        node.nodeType = nodeService.getType(childAssociationRef.getChildRef());
        node.isFolder = dictionaryService.isSubClass(node.nodeType, FOLDER_TYPE);
        node.isFile = dictionaryService.isSubClass(node.nodeType, FILE_TYPE);
        node.fullPath = parentFullPath + "/" + node.name;
        return node;
    }
}
