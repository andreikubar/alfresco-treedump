package com.andreikubar.alfresco.migration.export.filter;

import org.alfresco.model.ContentModel;
import org.alfresco.service.namespace.NamespaceService;
import org.alfresco.service.namespace.QName;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ExportFilters {
    public static Set<QName> ignoredProperties = new HashSet<>(
            Arrays.asList(
                    ContentModel.PROP_NODE_DBID,
                    ContentModel.PROP_NODE_UUID,
                    ContentModel.PROP_CATEGORIES,
                    ContentModel.ASPECT_TAGGABLE,
                    ContentModel.PROP_LOCK_TYPE,
                    ContentModel.PROP_LOCK_OWNER,
                    ContentModel.PROP_CONTENT
            ));

    public static Set<String> ignoredPropertyPrefixes = new HashSet<>(Arrays.asList("app", "exif"));


    public static Set<QName> ignoredAspects = new HashSet<>(Arrays.asList(
            ContentModel.ASPECT_TAGGABLE,
            ContentModel.ASPECT_CHECKED_OUT,
            ContentModel.ASPECT_LOCKABLE));

    public static Set<String> ignoredAspectPrefixes = new HashSet<>(Collections.singletonList("app"));

    public static boolean isIgnoredProperty(QName propertyQname, String prefixedPropertyName ){
        String propertyNamePrefix = prefixedPropertyName.substring(0, prefixedPropertyName.indexOf(":"));
        return ignoredProperties.contains(propertyQname) || ignoredPropertyPrefixes.contains(propertyNamePrefix);
    }

    public static boolean isIgnoredAspect(QName aspectName, String aspectPrefixedName){
        String aspectNamePrefix = aspectPrefixedName.substring(0, aspectPrefixedName.indexOf(":"));
        return ignoredAspects.contains(aspectName) || ignoredPropertyPrefixes.contains(aspectNamePrefix);
    }

}
