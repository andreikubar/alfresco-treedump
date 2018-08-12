package com.andreikubar.alfresco.migration.webscripts;

import com.andreikubar.alfresco.migration.exporter.ExportService;
import org.alfresco.repo.security.authentication.AuthenticationUtil;
import org.springframework.extensions.webscripts.AbstractWebScript;
import org.springframework.extensions.webscripts.WebScriptRequest;
import org.springframework.extensions.webscripts.WebScriptResponse;

import java.io.IOException;

public class ExportMetadataWebscript extends AbstractWebScript {

    private ExportService exportService;

    public void setExportService(ExportService exportService) {
        this.exportService = exportService;
    }

    @Override
    public void execute(WebScriptRequest webScriptRequest, WebScriptResponse webScriptResponse) throws IOException {
        AuthenticationUtil.setAdminUserAsFullyAuthenticatedUser();
        String nodeId = webScriptRequest.getParameter("nodeId");
        webScriptResponse.setContentType("application/octet-stream");
        webScriptResponse.getOutputStream().write(exportService.dumpMetadataToProtobuf(nodeId));
    }
}
