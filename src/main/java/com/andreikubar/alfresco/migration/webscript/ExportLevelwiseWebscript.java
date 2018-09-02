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

public class ExportLevelwiseWebscript extends DeclarativeWebScript {
    private Log log = LogFactory.getLog(ExportLevelwiseWebscript.class);
    private ExecutorService executorService;
    private Future<?> exportTask;
    private ExportEngine exportEngine;

    public ExportLevelwiseWebscript(ExportEngine exportEngine) {
        this.exportEngine = exportEngine;
    }

    @Override
    protected Map<String, Object> executeImpl(WebScriptRequest req, Status status, Cache cache) {
        Map<String, Object> model = new HashMap<>();

        if (executorService == null) {
            executorService = Executors.newSingleThreadExecutor();
        }
        if (exportTask == null || exportTask.isDone()) {
            exportTask = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        exportEngine.doLevelWiseExport();
                    }
                    catch (Exception e){
                        log.error("Failed to export all levels", e);
                    }
                }
            });
            model.put("result", "started");
        }
        else {
            model.put("result", "already running");
        }

        return model;
    }
}
