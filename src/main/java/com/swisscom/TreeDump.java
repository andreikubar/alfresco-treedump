package com.swisscom;

import org.springframework.extensions.webscripts.Cache;
import org.springframework.extensions.webscripts.DeclarativeWebScript;
import org.springframework.extensions.webscripts.Status;
import org.springframework.extensions.webscripts.WebScriptRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TreeDump extends DeclarativeWebScript {

    TreeDumpEngine treeDumpEngine;

    public TreeDump(TreeDumpEngine treeDumpEngine) {
        this.treeDumpEngine = treeDumpEngine;
    }

    @Override
    protected Map<String, Object> executeImpl(WebScriptRequest req, Status status, Cache cache) {
        Map<String, Object> model = new HashMap<>();
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                treeDumpEngine.startAndWait();
            }
        });

        model.put("result", "started");
        return model;
    }



}
