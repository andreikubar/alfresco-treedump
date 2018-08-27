package com.andreikubar.alfresco.migration.export.wireline;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;

public class ObjectTypeConverter {

    private static final ObjectTypeConverter instance = new ObjectTypeConverter();
    private static final Log LOG = LogFactory.getLog(ObjectTypeConverter.class);
    private Map<String, String> objectTypes = new HashMap<>();

    private ObjectTypeConverter() {
        objectTypes.put("sc:doc", "wln:document");
        objectTypes.put("sc:dossierContent", "wln:dossierContent");
        objectTypes.put("sc:dossierSpace", "wln:dossierSpace");
        objectTypes.put("sc:specificDossierSpace", "wln:specificDossierSpace");
        objectTypes.put("sc:specificDossierContainerSpace", "wln:specificDossierContainerSpace");
        objectTypes.put("sc:folder", "wln:folder");
        objectTypes.put("sc:dmReadonlySpace", "wln:dmReadonlySpace");
        objectTypes.put("sc:dmZIPArchiveSpace", "wln:dmZIPArchiveSpace");
        objectTypes.put("sc:orderContainer", "wln:orderContainer");
        objectTypes.put("sc:orderFinishedContainer", "wln:orderFinishedContainer");
        objectTypes.put("sc:orderArchivedContainer", "wln:orderArchivedContainer");
        objectTypes.put("sc:order", "wln:order");
        objectTypes.put("sc:variant", "	wln:variant");
        objectTypes.put("sc:dossierSubSpace", "wln:dossierSubSpace");
        objectTypes.put("sc:dmInboxSpace", "wln:dmInboxSpace");
        objectTypes.put("sc:attachmentSpace", "wln:attachmentSpace");
        objectTypes.put("sc:attachmentContainerSpace", "wln:attachmentContainerSpace");
    }

    public static ObjectTypeConverter getInstance() {
        return instance;
    }

    public String getNewObjectType(String oldObjectType) {
        if (objectTypes.containsKey(oldObjectType)) {
            return objectTypes.get(oldObjectType);
        } else {
            if (!oldObjectType.startsWith("cm:")) {
                LOG.warn("No matching object type found for: " + oldObjectType);
            }
            return oldObjectType;
        }
    }
}
