package com.andreikubar.alfresco.migration;

import org.alfresco.service.namespace.QName;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class CsvOutput {
    private Log log = LogFactory.getLog(CsvOutput.class);

    private String csvOutputDir;

    public CsvOutput(String csvOutputDir) {
        this.csvOutputDir = csvOutputDir;
        createOrCleanCsvOutDir();
    }

    private void createOrCleanCsvOutDir() {
        Path csvOutPath = Paths.get(csvOutputDir);
        if (!Files.exists(csvOutPath)) {
            try {
                Files.createDirectories(csvOutPath);
            } catch (IOException e) {
                log.error("Failed to create output directory", e);
            }
        }
        else {
            try {
                FileUtils.cleanDirectory(Paths.get(csvOutputDir).toFile());
            } catch (IOException e) {
                log.warn("Failed to clean the csv output directory");
            }
        }
    }

    public void writeCsvLine(BufferedWriter csvWriter, int level, String parentId, String childId, String childNodeType,
                             boolean isFolder, String fullPath, String nodeName,
                              String contentUrl, long contentBytes) throws IOException {
        String csvLine = String.format("%d|%s|%s|%s|%s|%s|%s|%s|%d\n",
                level,
                parentId,
                childId,
                childNodeType,
                isFolder ? "isFolder" : "notAFolder",
                fullPath,
                nodeName,
                contentUrl,
                contentBytes
        );
        csvWriter.append(csvLine);
    }

    public Path csvFilePathForLevel(String threadName, int level){
        return Paths.get(csvOutputDir,
                String.format("%s-%s-lvl-%d.csv", "treedump", threadName, level));
    }
}
