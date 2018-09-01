package com.andreikubar.alfresco.migration.webscript;

import org.alfresco.service.ServiceRegistry;
import org.alfresco.service.cmr.dictionary.DictionaryService;
import org.alfresco.service.namespace.NamespaceService;
import org.alfresco.service.namespace.QName;
import org.springframework.extensions.webscripts.Cache;
import org.springframework.extensions.webscripts.DeclarativeWebScript;
import org.springframework.extensions.webscripts.Status;
import org.springframework.extensions.webscripts.WebScriptRequest;

import java.util.*;

public class SubtypeListerWebscript extends DeclarativeWebScript {

    private ServiceRegistry serviceRegistry;
    private DictionaryService dictionaryService;
    private NamespaceService namespaceService;

    public SubtypeListerWebscript(ServiceRegistry serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
        this.dictionaryService = serviceRegistry.getDictionaryService();
        this.namespaceService = serviceRegistry.getNamespaceService();
    }

    @Override
    protected Map<String, Object> executeImpl(WebScriptRequest req, Status status, Cache cache) {
        Map<String, Object> model = new HashMap<>();
        String parentType = req.getServiceMatch().getTemplateVars().get("parent_type");
        QName parentTypeQName = QName.createQName(parentType, namespaceService);
        if (parentTypeQName == null){
            model.put("result", "type not found");
        }
        else {
            model.put("result", "found parent type QName " + parentTypeQName);
            Collection<QName> subTypes = dictionaryService.getSubTypes(parentTypeQName, true);
            List<QName> subTypesList = new ArrayList<>(subTypes);
            Collections.sort(subTypesList);
            model.put("qnames", subTypesList);
        }
        return model;
    }
}
