package com.andreikubar.alfresco.migration.exporter;

import com.andreikubar.alfresco.migration.exporter.model.ExportNode;
import org.alfresco.model.ContentModel;
import org.alfresco.service.ServiceRegistry;
import org.alfresco.service.cmr.dictionary.DictionaryService;
import org.alfresco.service.cmr.repository.ChildAssociationRef;
import org.alfresco.service.cmr.repository.ContentData;
import org.alfresco.service.cmr.repository.NodeRef;
import org.alfresco.service.cmr.repository.NodeService;
import org.alfresco.service.namespace.NamespaceService;
import org.alfresco.service.namespace.QName;


public class ExportNodeBuilder {
    private static final QName FOLDER_TYPE = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "folder");
    private static final QName FILE_TYPE = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "content");

    private NodeService nodeService;
    private DictionaryService dictionaryService;
    private NamespaceService namespaceService;
    private Boolean useCmName;

    public ExportNodeBuilder(ServiceRegistry serviceRegistry) {
        this.nodeService = serviceRegistry.getNodeService();
        this.dictionaryService = serviceRegistry.getDictionaryService();
        this.namespaceService = serviceRegistry.getNamespaceService();
    }

    public void setUseCmName(Boolean useCmName) {
        this.useCmName = useCmName;
    }

    public Boolean getUseCmName() {
        return useCmName;
    }

    public ExportNode constructExportNode(NodeRef nodeRef) {
        return constructExportNode(nodeRef, null);
    }

    public ExportNode constructExportNode(ChildAssociationRef childAssociationRef, String parentFullPath){
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

    public ExportNode constructExportNode(NodeRef nodeRef, String parentFullPath){
        ExportNode node = new ExportNode();
        node.nodeRef = nodeRef;
        node.nodeType = nodeService.getType(nodeRef);
        node.typePrefixed = node.nodeType.getPrefixString();
        node.isFile = dictionaryService.isSubClass(node.nodeType, FOLDER_TYPE);
        node.isFile = dictionaryService.isSubClass(node.nodeType, FILE_TYPE);
        node.name = (String) nodeService.getProperty(nodeRef, ContentModel.PROP_NAME);
        if (parentFullPath != null) {
            node.fullPath = parentFullPath + "/" + node.name; //TODO build correct prefixed path
        }
        else {
            node.fullPath = nodeService.getPath(nodeRef).toPrefixString(namespaceService);
        }
        if (node.isFile) {
            ContentData contentData = (ContentData) nodeService.getProperty(nodeRef,
                    QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "content"));
            node.url = contentData.getContentUrl().replace("store://", ""); //TODO match from the beginning of the string
        }
        return node;
    }
}
