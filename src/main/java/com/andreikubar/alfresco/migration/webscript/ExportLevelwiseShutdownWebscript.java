package com.andreikubar.alfresco.migration.webscript;

import com.andreikubar.alfresco.migration.ExportEngine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.extensions.webscripts.Cache;
import org.springframework.extensions.webscripts.DeclarativeWebScript;
import org.springframework.extensions.webscripts.Status;
import org.springframework.extensions.webscripts.WebScriptRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ExportLevelwiseShutdownWebscript extends DeclarativeWebScript {
    private Log log = LogFactory.getLog(ExportLevelwiseShutdownWebscript.class);
    private ExportEngine exportEngine;

    public ExportLevelwiseShutdownWebscript(ExportEngine exportEngine) {
        this.exportEngine = exportEngine;
    }

    @Override
    protected Map<String, Object> executeImpl(WebScriptRequest req, Status status, Cache cache) {
        Map<String, Object> model = new HashMap<>();
        exportEngine.shutdown();
        model.put("result", "shutdown");
        return model;
    }
}
