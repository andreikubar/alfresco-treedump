package com.andreikubar.alfresco.migration.export;

import com.andreikubar.alfresco.migration.proto.ExportProtos;
import org.alfresco.model.ContentModel;
import org.alfresco.service.ServiceRegistry;
import org.alfresco.service.cmr.repository.ContentData;
import org.alfresco.service.cmr.repository.MLText;
import org.alfresco.service.cmr.repository.NodeService;
import org.alfresco.service.cmr.security.AccessPermission;
import org.alfresco.service.cmr.security.PermissionService;
import org.alfresco.service.namespace.NamespaceService;
import org.alfresco.service.namespace.QName;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.andreikubar.alfresco.migration.export.filter.ExportFilters.isIgnoredAspect;
import static com.andreikubar.alfresco.migration.export.filter.ExportFilters.isIgnoredProperty;

public class ProtoExportService {
    private Log log = LogFactory.getLog(ProtoExportService.class);

    private NamespaceService namespaceService;
    private NodeService nodeService;
    private PermissionService permissionService;

    public ProtoExportService(ServiceRegistry serviceRegistry) {
        this.namespaceService = serviceRegistry.getNamespaceService();
        this.nodeService = serviceRegistry.getNodeService();
        this.permissionService = serviceRegistry.getPermissionService();
    }

    public void exportNodeToFile(ExportNode exportNode, BufferedOutputStream outputStream) throws IOException {
        ExportProtos.AlfrescoNode protoMessage = constructProtoMessage(exportNode);
        protoMessage.writeTo(outputStream);
        //log.debug(String.format("Wrote %d bytes for node %s",protoMessage.getSerializedSize(), exportNode.fullPath));
        //log.debug(String.format("Protobuf contents for node %s:\n%s", exportNode.fullPath, protoMessage.toString()));
    }

    private ExportProtos.AlfrescoNode constructProtoMessage(ExportNode exportNode) {
        ExportProtos.AlfrescoNode.Builder alfrescoNodeBuilder = ExportProtos.AlfrescoNode.newBuilder();
        addProtoProperties(exportNode, alfrescoNodeBuilder);
        addProtoAspects(exportNode, alfrescoNodeBuilder);
        addProtoPermissions(exportNode, alfrescoNodeBuilder);
        addTranlatedProperties(exportNode, alfrescoNodeBuilder);
        addContentData(exportNode, alfrescoNodeBuilder);
        alfrescoNodeBuilder
                .setInheritsPermissions(permissionService.getInheritParentPermissions(exportNode.nodeRef))
                .setName(exportNode.name)
                .setFullPath(exportNode.fullPath)
                .setType(exportNode.nodeTypePrefixed);
        return alfrescoNodeBuilder.build();
    }

    private void addContentData(ExportNode exportNode, ExportProtos.AlfrescoNode.Builder alfrescoNodeBuilder) {
        if (exportNode.isFile){
            Serializable contentDataProperty = exportNode.properties.get(ContentModel.PROP_CONTENT);
            if (contentDataProperty == null){
                log.error("Node is of File type, but ContentData is missing " + exportNode.fullPath);
                return;
            }
            ContentData contentData = (ContentData) contentDataProperty;
            ExportProtos.AlfrescoNode.ContentData.Builder protoContentData =
                    ExportProtos.AlfrescoNode.ContentData.newBuilder();
            if (contentData.getContentUrl() != null){
                protoContentData.setContentUrl(contentData.getContentUrl());
            }else {
                log.error("Content URL was null for " + exportNode.fullPath);
            }
            if (contentData.getMimetype() != null){
                protoContentData.setMimeType(contentData.getMimetype());
            }
            else {
                log.error("Mimetype was null for " + exportNode.fullPath);
            }
            if (contentData.getEncoding() != null){
                protoContentData.setEncoding(contentData.getEncoding());
            }
            else {
                log.warn("Encoding was null for " + exportNode.fullPath);
            }
            if (contentData.getLocale() != null){
                protoContentData.setLocale(contentData.getLocale().toLanguageTag());
            }
            else {
                log.debug("Locale was null for " + exportNode.fullPath);
            }
            protoContentData.setSize(contentData.getSize());

            if (protoContentData.hasContentUrl() && protoContentData.hasMimeType()) {
                alfrescoNodeBuilder.setContentData(
                        protoContentData.build()
                );
            }
            else {
                throw new RuntimeException(
                        String.format("Failed to transfer ContentData property for %s, URL or mimetype was null",
                                exportNode.fullPath));
            }
        }
    }

    private void addTranlatedProperties(ExportNode exportNode, ExportProtos.AlfrescoNode.Builder alfrescoNodeBuilder) {
        for (Map.Entry<QName, Serializable> prop : exportNode.properties.entrySet()) {
            if (prop.getValue() instanceof MLText) {
                MLText mlText = (MLText) prop.getValue();
                ExportProtos.AlfrescoNode.TranslatedProperty.Builder translatedProperty =
                        ExportProtos.AlfrescoNode.TranslatedProperty.newBuilder();
                translatedProperty.setName(prop.getKey().getLocalName());
                for (Locale locale : mlText.getLocales()) {
                    String translatedText = mlText.getClosestValue(locale);
                    ExportProtos.AlfrescoNode.Translation.Builder translation =
                            ExportProtos.AlfrescoNode.Translation.newBuilder();
                    translation.setLocale(locale.getLanguage());
                    if (StringUtils.isNotBlank(translatedText)){
                        translation.setTranslation(translatedText);
                    }
                    else {
                        log.warn("Translation with empty value for node " + exportNode.fullPath +
                                " translated property: " + prop.getKey().toPrefixString(namespaceService));
                    }
                    translatedProperty.addTranslations(
                            translation.build()
                    );
                }
                alfrescoNodeBuilder.addTranslatedProperties(
                        translatedProperty.build()
                );
            }
        }
    }

    private void addProtoPermissions(ExportNode exportNode, ExportProtos.AlfrescoNode.Builder alfrescoNodeBuilder) {
        Set<AccessPermission> accessPermissions = permissionService.getAllSetPermissions(exportNode.nodeRef);
        for (AccessPermission permission : accessPermissions) {
            alfrescoNodeBuilder.addPermissions(
                    ExportProtos.AlfrescoNode.Permission.newBuilder()
                            .setAuthority(permission.getAuthority())
                            .setPermission(permission.getPermission())
                            .setAccessStatus(permission.getAccessStatus().toString())
                            .setIsInherited(permission.isInherited())
                            .build());
        }
    }

    private void addProtoAspects(ExportNode exportNode, ExportProtos.AlfrescoNode.Builder alfrescoNodeBuilder) {
        Set<QName> aspects = nodeService.getAspects(exportNode.nodeRef);
        for (QName aspect : aspects) {
            String aspectPrefixedName = aspect.toPrefixString(namespaceService);
            if (isIgnoredAspect(aspect, aspectPrefixedName)) {
                continue;
            }
            alfrescoNodeBuilder.addAspects(
                    ExportProtos.AlfrescoNode.Aspect.newBuilder()
                            .setName(aspectPrefixedName)
                            .build());
        }
    }

    private void addProtoProperties(ExportNode exportNode, ExportProtos.AlfrescoNode.Builder alfrescoNodeBuilder) {
        for (Map.Entry<QName, Serializable> propertyEntry : exportNode.properties.entrySet()) {
            QName propertyQname = propertyEntry.getKey();
            String prefixedPropertyName = propertyEntry.getKey().toPrefixString(namespaceService);
            if (isIgnoredProperty(propertyQname, prefixedPropertyName)) {
                continue;
            }
            String propertyValue = formatPropertyValue(propertyEntry.getValue());
            if (StringUtils.isNotBlank(propertyValue)) {
                alfrescoNodeBuilder.addProperties(
                        ExportProtos.AlfrescoNode.Property.newBuilder()
                                .setName(prefixedPropertyName)
                                .setValue(propertyValue)
                                .build());
            }
        }
    }

    private String formatPropertyValue(Serializable value) {
        String returnValue = "";

        if (value != null) {
            if (value instanceof Date) {
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSSZ");
                Date date = (Date) value;
                returnValue = format.format(date);
                returnValue = returnValue.substring(0, 26) + ":" + returnValue.substring(26);
            } else if (value instanceof MLText) {
                MLText mlText = (MLText) value;
                returnValue = mlText.getClosestValue(Locale.forLanguageTag("de"));
            } else {
                returnValue = value.toString();
            }
        }

        return returnValue;
    }
}
