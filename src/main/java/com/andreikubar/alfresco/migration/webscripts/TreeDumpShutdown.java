package com.andreikubar.alfresco.migration.webscripts;

import com.andreikubar.alfresco.migration.TreeDumpEngine;
import org.springframework.extensions.webscripts.Cache;
import org.springframework.extensions.webscripts.DeclarativeWebScript;
import org.springframework.extensions.webscripts.Status;
import org.springframework.extensions.webscripts.WebScriptRequest;

import java.util.HashMap;
import java.util.Map;

public class TreeDumpShutdown extends DeclarativeWebScript {

    private TreeDumpEngine treeDumpEngine;

    public TreeDumpShutdown(TreeDumpEngine treeDumpEngine) {
        this.treeDumpEngine = treeDumpEngine;
    }

    @Override
    protected Map<String, Object> executeImpl(WebScriptRequest req, Status status, Cache cache) {
        Map<String, Object> model = new HashMap<>();
        this.treeDumpEngine.shutdown();
        model.put("result", "shutdown");
        return model;
    }
}
