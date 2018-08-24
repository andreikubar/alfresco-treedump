package com.andreikubar.alfresco.migration.export;

import org.alfresco.model.ContentModel;
import org.alfresco.repo.node.MLPropertyInterceptor;
import org.alfresco.service.ServiceRegistry;
import org.alfresco.service.cmr.dictionary.DictionaryService;
import org.alfresco.service.cmr.repository.ChildAssociationRef;
import org.alfresco.service.cmr.repository.ContentData;
import org.alfresco.service.cmr.repository.NodeRef;
import org.alfresco.service.cmr.repository.NodeService;
import org.alfresco.service.namespace.NamespaceService;
import org.alfresco.service.namespace.QName;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


public class ExportNodeBuilder {
    private static final QName FOLDER_TYPE = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "folder");
    private static final QName FILE_TYPE = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "content");

    private ServiceRegistry serviceRegistry;
    private NodeService nodeService;
    private Boolean useCmName;
    private Boolean readProperties = false;

    public void setServiceRegistry(ServiceRegistry serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
    }

    public void setUseCmName(Boolean useCmName) {
        this.useCmName = useCmName;
    }

    public Boolean getUseCmName() {
        return useCmName;
    }

    public void setReadProperties(Boolean readProperties) {
        this.readProperties = readProperties;
    }

    public ExportNode constructExportNode(ChildAssociationRef childAssociationRef, String parentFullPath){
        nodeService = serviceRegistry.getNodeService();
        DictionaryService dictionaryService = serviceRegistry.getDictionaryService();

        if (useCmName == null) useCmName = true;

        ExportNode node = new ExportNode();
        node.nodeRef = childAssociationRef.getChildRef();
        node.nodeType = nodeService.getType(childAssociationRef.getChildRef());
        node.isFolder = dictionaryService.isSubClass(node.nodeType, FOLDER_TYPE);
        node.isFile = dictionaryService.isSubClass(node.nodeType, FILE_TYPE);

        if (readProperties){
            node.properties = readProperties(node.nodeRef);
            if (node.properties.containsKey(ContentModel.PROP_CONTENT)){
                ContentData contentData = (ContentData) node.properties.get(ContentModel.PROP_CONTENT);
                node.contentUrl = contentData.getContentUrl();
                node.contentBytes = contentData.getSize();
            }
        }
        else {
            node.properties = new HashMap<>();
        }

        node.name = makeNodeName(node, childAssociationRef.getQName().getLocalName());
        node.fullPath = parentFullPath + "/" + node.name;
        return node;
    }

    private String makeNodeName(ExportNode node, String localName){
        String name;
        if (readProperties){
             name = ((String)node.properties.get(ContentModel.PROP_NAME));
        }
        else {
            if (useCmName) {
                name  = (String)nodeService.getProperty(node.nodeRef, ContentModel.PROP_NAME);
            }
            else {
                name = localName;
            }
        }
        return replaceIllegalChars(name);
    }

    private String replaceIllegalChars(String filename){
        StringBuilder newName = new StringBuilder();
        for (int i = 0; i < filename.length(); i++){
            char currentChar = filename.charAt(i);
            if (currentChar < ' ' || "<>:\"|?*".indexOf(currentChar) != -1){
                newName.append('_');
            }
            else {
                newName.append(currentChar);
            }
        }
        return newName.toString();
    }

    private Map<QName, Serializable> readProperties(NodeRef nodeRef){
        MLPropertyInterceptor.setMLAware(true);
        Map<QName, Serializable> properties = nodeService.getProperties(nodeRef);
        MLPropertyInterceptor.setMLAware(false);
        return properties;
    }
}
