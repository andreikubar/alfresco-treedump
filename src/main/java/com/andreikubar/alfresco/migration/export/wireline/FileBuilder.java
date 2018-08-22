package com.andreikubar.alfresco.migration.export.wireline;

import org.alfresco.service.cmr.security.AccessPermission;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public final class FileBuilder {

    private static final String HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
    private static final String PROPERTIES_SUB_HEADER = "<!DOCTYPE properties SYSTEM \"http://java.sun" +
            ".com/dtd/properties.dtd\">\n<properties>";
    private static final String ACL_SUB_HEADER = "<acl inherited=\"%s\">\n";
    private static final String PROPERTIES_FOOTER = "\n</properties>";
    private static final String ACL_FOOTER = "</acl>";

    private FileBuilder() {
    }

    static String buildMetadataFileContent(String type, List<String> aspects, Map<String, String> properties) {
        String tType = "<entry key=\"type\">" + type + "</entry>";
        String tAspect = "<entry key=\"aspects\">" + formatAspects(aspects) + "</entry>";
        StringBuilder sb = new StringBuilder();

        sb.append("\n\t" + tType + "\n\t" + tAspect);

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String value = htmlEncode(entry.getValue());
            sb.append("\n\t<entry key=\"" + entry.getKey() + "\">" + value + "</entry>");
        }
        return buildFile(sb.toString());
    }

    static String buildAclFileContent(Collection<AccessPermission> permissions, boolean inherited) {
        String aceTag = "\t<ace authority=\"%s\" permission=\"%s\" position=\"%s\" access-status=\"%s\" inherited=\"%s\"/>";
        StringBuilder sb = new StringBuilder();

        for (AccessPermission permission : permissions) {
            String tag = String.format(aceTag, permission.getAuthority(), convertPermission(permission.getPermission
                    ()), permission.getPosition(), permission.getAccessStatus(), permission.isInherited());
            sb.append(tag);
            sb.append("\n");
        }
        return buildFile(sb.toString(), inherited);
    }

    static String buildTranslationFileContent(Map<String, List<Translation>> translationMap) {
        String languagesTag = "{\"tags\": [\n";
        StringBuilder builder = new StringBuilder();
        builder.append(languagesTag);
        Iterator<Map.Entry<String, List<Translation>>> tagIter = translationMap.entrySet().iterator();
        while (tagIter.hasNext()) {
            Map.Entry<String, List<Translation>> entry = tagIter.next();
            builder.append(String.format("\t { \"%s\": [\n", entry.getKey()));
            Iterator<Translation> iter = entry.getValue().iterator();
            while (iter.hasNext()) {
                Translation translation = iter.next();
                builder.append(String.format("\t\t{\"locale\": \"%s\",\n", translation.getLocale()));
                builder.append(String.format("\t\t\"translation\": \"%s\"}", translation.getTranslation()));
                if (!iter.hasNext()) {
                    builder.append("\n");
                } else {
                    builder.append(",\n");
                }
            }
            if (!tagIter.hasNext()) {
                builder.append("\t]}\n");
            } else {
                builder.append("\t]},\n");
            }
        }
        builder.append("]}\n");
        return builder.toString();
    }

    private static String buildFile(String text) {
        StringBuilder builder = new StringBuilder();
        builder.append(HEADER);
        builder.append(PROPERTIES_SUB_HEADER);
        builder.append(text);
        builder.append(PROPERTIES_FOOTER);
        return builder.toString();
    }

    private static String buildFile(String text, boolean inherited) {
        StringBuilder builder = new StringBuilder();
        builder.append(HEADER);
        builder.append(String.format(ACL_SUB_HEADER, inherited));
        builder.append(text);
        builder.append(ACL_FOOTER);
        return builder.toString();
    }

    private static String formatAspects(List<String> aspects) {
        String dado = "";
        boolean flag = false;
        for (String string : aspects) {
            if (flag) {
                dado += ",";
            }
            dado += string;
            flag = true;
        }
        return dado;
    }

    private static String htmlEncode(String value) {
        //format &
        value = value.replaceAll("&", "&amp;");
        //format < and >
        value = value.replaceAll("<", "&lt;").replaceAll(">", "&gt;");

        return value;
    }

    private static String convertPermission(String permission) {
        return permission.replace("sc_", "wln_").replace("Contributor_simple", "ContributorSimple");
    }
}
