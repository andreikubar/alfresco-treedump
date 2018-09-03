package com.andreikubar.alfresco.migration.export.wireline;

import com.andreikubar.alfresco.migration.export.ExportNode;
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

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.andreikubar.alfresco.migration.export.filter.ExportFilters.ignoredAspects;
import static com.andreikubar.alfresco.migration.export.filter.ExportFilters.ignoredProperties;
import static com.andreikubar.alfresco.migration.export.filter.ExportFilters.ignoredPropertyPrefixes;

public class WirelineExportService {
    private Log log = LogFactory.getLog(WirelineExportService.class);

    ServiceRegistry serviceRegistry;
    NodeService nodeService;
    NamespaceService namespaceService;
    PermissionService permissionService;

    public WirelineExportService(ServiceRegistry serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
        this.nodeService = serviceRegistry.getNodeService();
        this.namespaceService = serviceRegistry.getNamespaceService();
        this.permissionService = serviceRegistry.getPermissionService();
    }

    public void createDirectory(ExportNode node, Path datedExportFolder) {
        try {
            Path pathToCreate = node.isFolder ? Paths.get(node.fullPath) : Paths.get(node.fullPath).getParent();
            if (pathToCreate.getRoot() != null) {
                pathToCreate = pathToCreate.subpath(0, pathToCreate.getNameCount());
            }
            Files.createDirectories(datedExportFolder.resolve(pathToCreate));
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }

    public void writePropertyAndTranslationsFile(ExportNode node, Path exportDestination) {
        Map<String, String> convertedProperties = filterAndConvertWirelineProperties(node.properties);
        Set<QName> aspects = nodeService.getAspects(node.nodeRef);
        List<String> convertedAspects = filterAndConvertWirelineAspects(aspects);

        ObjectTypeConverter objectTypeConverter = ObjectTypeConverter.getInstance();
        String convertedNodeType = objectTypeConverter.getNewObjectType(
                node.nodeType.toPrefixString(namespaceService)
        );

        String xmlContent = FileBuilder.buildMetadataFileContent(convertedNodeType, convertedAspects,
                convertedProperties);

        Path metadataFile = constructMetadataPath(node, exportDestination, ".metadata.properties.xml");
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(metadataFile.toFile()), "UTF8"))) {
            bw.write(xmlContent);
        } catch (IOException e) {
            log.error("Failed to write metadata file " + metadataFile.toString());
            throw new RuntimeException(e);
        }

        Map<String, List<Translation>> translations = getMultilingualProperties(node.properties);
        writeTranslationsFile(node, translations, exportDestination);
    }

    public void writeAclFile(ExportNode node, Path exportDestination) {
        Set<AccessPermission> accessPermissions = permissionService.getAllSetPermissions(node.nodeRef);
        boolean inherited = permissionService.getInheritParentPermissions(node.nodeRef);
        String xmlContent = FileBuilder.buildAclFileContent(accessPermissions, inherited);
        Path aclFile = constructMetadataPath(node, exportDestination, ".acl");
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(aclFile.toFile()), "UTF8"))) {
            bw.write(xmlContent);
        } catch (IOException e) {
            log.error("Failed to write acl file " + aclFile.toString());
            throw new RuntimeException(e);
        }
    }

    public void writeTranslationsFile(ExportNode node, Map<String, List<Translation>> translations, Path exportDestination) {
        String xmlContent = FileBuilder.buildTranslationFileContent(translations);
        Path translationsFile = constructMetadataPath(node, exportDestination, ".locale.json");
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(translationsFile.toFile()), "UTF8"))) {
            bw.write(xmlContent);
        } catch (IOException e) {
            log.error("Failed to write translations file " + translationsFile.toString());
            throw new RuntimeException(e);
        }
    }

    private Path constructMetadataPath(ExportNode node, Path exportDestination, String suffix) {
        Path metadataPath = exportDestination.resolve(
                (getRelativeNodePath(node)) + suffix
        );
        if (metadataPath.toAbsolutePath().toString().getBytes().length > 260) {
            String nodePathShort = getNodePathNodeRefBased(node);
            metadataPath = exportDestination.resolve(nodePathShort + suffix);
        }
        return metadataPath;
    }

    public Map<String, List<Translation>> getMultilingualProperties(Map<QName, Serializable> props) {
        Map<String, List<Translation>> translationMap = new HashMap<>();
        for (Map.Entry<QName, Serializable> prop : props.entrySet()) {
            if (prop.getValue() instanceof MLText) {
                MLText mlText = (MLText) prop.getValue();
                List<Translation> translations = new ArrayList<>();
                for (Locale locale : mlText.getLocales()) {
                    translations.add(new Translation(mlText.getClosestValue(locale), locale.getLanguage()));
                }
                translationMap.put(prop.getKey().getLocalName(), translations);
            }
        }
        return translationMap;
    }

    private Map<String, String> filterAndConvertWirelineProperties(Map<QName, Serializable> alfrescoProperties) {
        Map<String, String> convertedProperties = new HashMap<>();
        for (Map.Entry<QName, Serializable> propertyEntry : alfrescoProperties.entrySet()) {
            QName propertyQname = propertyEntry.getKey();
            String prefixedPropertyName = propertyEntry.getKey().toPrefixString(namespaceService);
            String propertyNamePrefix = prefixedPropertyName.substring(0, prefixedPropertyName.indexOf(":"));
            if (ignoredProperties.contains(propertyQname)) {
                continue;
            }
            if (ignoredPropertyPrefixes.contains(propertyNamePrefix)) {
                continue;
            }
            String propertyNameConverted = prefixedPropertyName.replace("sc:", "wln:");

            String propertyValue = "";
            try {
                propertyValue = formatMetadata(propertyEntry.getValue()).replace("sc:", "wln:");
            } catch (NullPointerException e) {
                log.debug("Got null value for property " + prefixedPropertyName);
            }

            if (!StringUtils.isBlank(propertyValue)) {
                convertedProperties.put(propertyNameConverted, propertyValue);
            }
        }
        return convertedProperties;
    }

    private List<String> filterAndConvertWirelineAspects(Set<QName> aspects) {
        List<String> convertedAspects = new ArrayList<>(aspects.size());

        for (QName aspect : aspects) {
            if (ignoredAspects.contains(aspect)) {
                continue;
            }
            String aspectPrefixedName = aspect.toPrefixString(namespaceService);
            String aspectNamePrefix = aspectPrefixedName.substring(0, aspectPrefixedName.indexOf(":"));
            if (ignoredPropertyPrefixes.contains(aspectNamePrefix)) {
                continue;
            }
            String convertedAspectName = aspectPrefixedName.replace("sc:", "wln:");
            convertedAspects.add(convertedAspectName);
        }
        return convertedAspects;
    }

    private String formatMetadata(Serializable obj) {
        String returnValue = "";

        if (obj != null) {
            if (obj instanceof Date) {
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSSZ");
                Date date = (Date) obj;
                returnValue = format.format(date);
                returnValue = returnValue.substring(0, 26) + ":" + returnValue.substring(26);
            } else if (obj instanceof MLText) {
                MLText mlText = (MLText) obj;
                returnValue = mlText.getClosestValue(Locale.forLanguageTag("de"));
            } else if (obj instanceof ContentData) {
                ContentData contentData = (ContentData) obj;
                returnValue = contentData.toString();
            } else {
                returnValue = obj.toString();
            }
        }

        return returnValue;
    }

    private String getRelativeNodePath(ExportNode node) {
        return node.fullPath.startsWith("/") ? node.fullPath.substring(1) : node.fullPath;
    }

    private String getNodePathNodeRefBased(ExportNode node) {
        Path nodeFullPath = Paths.get(getRelativeNodePath(node));
        String nodeFullPathWithNodeRefAsFileName = nodeFullPath.getParent().toString() + "/" + node.nodeRef.getId();
        return nodeFullPathWithNodeRefAsFileName;
    }
}
