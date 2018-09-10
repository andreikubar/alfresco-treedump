package com.andreikubar.alfresco.migration;

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

public class TreeDump extends DeclarativeWebScript {
    private Log log = LogFactory.getLog(TreeDump.class);

    private TreeDumpEngine treeDumpEngine;
    private ExecutorService executorService;
    private Future<?> treeDumpTask;

    public TreeDump(TreeDumpEngine treeDumpEngine) {
        this.treeDumpEngine = treeDumpEngine;
    }

    @Override
    protected Map<String, Object> executeImpl(WebScriptRequest req, Status status, Cache cache) {
        Map<String, Object> model = new HashMap<>();
        if (executorService == null) {
            executorService = Executors.newSingleThreadExecutor();
        }
        if (treeDumpTask == null || treeDumpTask.isDone()) {
            treeDumpTask = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        treeDumpEngine.startAndWait();
                    }
                    catch (Exception e){
                        log.error("Error while processing treedump", e);
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
