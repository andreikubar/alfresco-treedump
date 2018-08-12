package com.andreikubar.alfresco.migration.webscripts;

import com.andreikubar.alfresco.migration.importer.ImportEngine;
import org.springframework.extensions.webscripts.Cache;
import org.springframework.extensions.webscripts.DeclarativeWebScript;
import org.springframework.extensions.webscripts.Status;
import org.springframework.extensions.webscripts.WebScriptRequest;

import java.util.HashMap;
import java.util.Map;

public class InitiateImpotWebscript extends DeclarativeWebScript {

    private ImportEngine importEngine;

    public void setImportEngine(ImportEngine importEngine) {
        this.importEngine = importEngine;
    }

    @Override
    protected Map<String, Object> executeImpl(WebScriptRequest req, Status status, Cache cache) {
        Map<String, Object> model = new HashMap<>();
        String startNodeId = req.getParameter("startNodeId");

        return model;
    }
}
