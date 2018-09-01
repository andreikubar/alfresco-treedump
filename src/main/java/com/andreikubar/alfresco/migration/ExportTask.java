package com.andreikubar.alfresco.migration;

import com.andreikubar.alfresco.migration.export.ExportNode;
import org.alfresco.repo.security.authentication.AuthenticationUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.andreikubar.alfresco.migration.ExportEngine.levelThreadWriters;

public class ExportTask implements Runnable {
    private Log log = LogFactory.getLog(ExportTask.class);
    private List<ExportNode> exportNodes;
    private Integer level;
    private BufferedWriter threadWriter;

    public ExportTask(List<ExportNode> exportNodes, Integer level) {
        this.exportNodes = exportNodes;
        this.level = level;
    }

    @Override
    public void run() {
        initThreadWriter();
        AuthenticationUtil.setAdminUserAsFullyAuthenticatedUser();
        for (ExportNode exportNode : exportNodes) {
            try {
                ExportEngine.csvOutput.writeCsvLine(
                        threadWriter,
                        level,
                        "",
                        exportNode.nodeRef.getId(),
                        exportNode.nodeTypePrefixed,
                        exportNode.isFolder,
                        exportNode.fullPath,
                        exportNode.name,
                        exportNode.contentUrl,
                        exportNode.contentBytes);
            } catch (IOException e) {
                log.error("Failed to write CSV line for child " + exportNode.fullPath);
            }
        }
    }

    private void initThreadWriter() {
        String threadName = Thread.currentThread().getName();
        Map<String, BufferedWriter> threadWriters;
        if (!levelThreadWriters.containsKey(level)){
            levelThreadWriters.put(level, new HashMap<String, BufferedWriter>());
        }
        threadWriters = levelThreadWriters.get(level);
        if (!threadWriters.containsKey(threadName)) {
            try {
                Path csvOutFile = ExportEngine.csvOutput.csvFilePathForLevel(threadName, level);
                BufferedWriter csvWriter = new BufferedWriter(
                        new OutputStreamWriter(
                                new FileOutputStream(csvOutFile.toFile(), true), StandardCharsets.UTF_8));
                threadWriters.put(threadName, csvWriter);
            } catch (FileNotFoundException e) {
                log.error("Failed to open BufferedWriter", e);
                throw new RuntimeException(e);
            }
        }
        this.threadWriter = threadWriters.get(threadName);
    }
}
