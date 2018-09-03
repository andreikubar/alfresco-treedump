package com.andreikubar.alfresco.migration;

import com.andreikubar.alfresco.migration.export.ExportNode;
import com.andreikubar.alfresco.migration.export.ExportNodeBuilder;
import com.andreikubar.alfresco.migration.export.ProtoExportService;
import com.andreikubar.alfresco.migration.export.wireline.WirelineExportService;
import org.alfresco.model.ContentModel;
import org.alfresco.repo.content.ContentStore;
import org.alfresco.repo.security.authentication.AuthenticationUtil;
import org.alfresco.service.ServiceRegistry;
import org.alfresco.service.cmr.repository.ChildAssociationRef;
import org.alfresco.service.cmr.repository.NodeRef;
import org.alfresco.service.cmr.repository.NodeService;
import org.alfresco.service.cmr.repository.StoreRef;
import org.alfresco.service.namespace.NamespaceService;
import org.alfresco.service.namespace.RegexQNamePattern;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class ExportEngine {
    public static final int BATCH_SIZE = 50;
    public static CsvOutput csvOutput;
    private Log log = LogFactory.getLog(ExportEngine.class);

    private String rootNodeListInput;
    private String propertyFile;
    private String exportDestination;
    private String csvOutputDir;
    private int numberOfThreads;

    private NodeService nodeService;
    private NamespaceService namespaceService;
    private ContentStore defaultContentStore;
    private WirelineExportService wirelineExportService;
    private ProtoExportService protoExportService;
    private ExportNodeBuilder exportNodeBuilder;

    private ExecutorService executorService;
    private CompletionService<Integer> completionService;
    private Map<Integer, Map<String, BufferedWriter>> levelThreadWriters = new HashMap<>();
    private Map<Integer, Map<String, BufferedOutputStream>> levelProtoStreams = new HashMap<>();
    private Path datedExportFolder;

    public ExportEngine(ServiceRegistry serviceRegistry, ContentStore defaultContentStore) {
        this.defaultContentStore = defaultContentStore;
        nodeService = serviceRegistry.getNodeService();
        namespaceService = serviceRegistry.getNamespaceService();
    }

    public void setExportNodeBuilder(ExportNodeBuilder exportNodeBuilder) {
        this.exportNodeBuilder = exportNodeBuilder;
    }

    public void setWirelineExportService(WirelineExportService wirelineExportService) {
        this.wirelineExportService = wirelineExportService;
    }

    public void setProtoExportService(ProtoExportService protoExportService) {
        this.protoExportService = protoExportService;
    }

    public void setPropertyFile(String propertyFile) {
        this.propertyFile = propertyFile;
    }

    private void loadPropeties() {
        Properties properties = new Properties();
        InputStream input = null;
        try {
            input = new FileInputStream(this.propertyFile);
            properties.load(input);

            if (StringUtils.isBlank(this.csvOutputDir = properties.getProperty("csvOutputDir"))) {
                throw new IllegalArgumentException("parameter csvOutputDir is missing");
            }
            if (StringUtils.isBlank(this.rootNodeListInput = properties.getProperty("rootNodeListInput"))) {
                throw new IllegalArgumentException("parameter rootNodeListInput is missing");
            }
            if (StringUtils.isBlank(this.exportDestination = properties.getProperty("exportDestination"))) {
                throw new IllegalArgumentException("parameter exportDestination is missing");
            }
            this.numberOfThreads = Integer.parseInt(properties.getProperty("numberOfThreads",
                    String.valueOf(Runtime.getRuntime().availableProcessors())));
        } catch (FileNotFoundException e) {
            log.error("Property file not found " + this.propertyFile);
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void doLevelWiseExport() {
        AuthenticationUtil.setAdminUserAsFullyAuthenticatedUser();
        this.exportNodeBuilder.setReadProperties(true);
        loadPropeties();
        csvOutput = new CsvOutput(csvOutputDir);
        levelThreadWriters = new HashMap<>();
        if (executorService == null || executorService.isShutdown()) {
            executorService = Executors.newFixedThreadPool(numberOfThreads);
        }
        completionService = new ExecutorCompletionService<>(executorService);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HHmm");
        Date date = new Date();
        String datedSubfolderName = format.format(date);
        this.datedExportFolder = Paths.get(exportDestination).resolve(datedSubfolderName);

        List<ExportNode> startNodes = getStartNodes();
        List<List<ExportNode>> levels = new ArrayList<>();
        levels.add(startNodes);

        long startTime = System.currentTimeMillis();
        List<ExportNode> nextLevelNodes;
        for (int level = 0; ; level++) {
            log.info("Starting level " + level + " export");
            nextLevelNodes = new ArrayList<>();
            levels.add(nextLevelNodes);
            int nodeCounter = 0;
            int lastDispatched = 0;
            List<Future> exportTasks = new ArrayList<>();
            for (ExportNode parentNode : levels.get(level)) {
                if (!parentNode.isFolder) {
                    continue;
                }
                List<ChildAssociationRef> childAssocs = nodeService.getChildAssocs(parentNode.nodeRef,
                        ContentModel.ASSOC_CONTAINS, RegexQNamePattern.MATCH_ALL);
                if (childAssocs.size() > 0) {
                    for (ChildAssociationRef childAssoc : childAssocs) {
                        nextLevelNodes.add(exportNodeBuilder.constructExportNode(childAssoc, parentNode.fullPath));
                        nodeCounter++;
                        if (nodeCounter - lastDispatched >= BATCH_SIZE) {
                            exportTasks.add(
                                    dispatchCurrentBatch(
                                            new ArrayList<>(nextLevelNodes.subList(lastDispatched, nodeCounter)), level));
                            log.debug("Tasks submitted: " + exportTasks.size());
                            lastDispatched = nodeCounter;
                        }
                    }
                }
            }
            if (nodeCounter > lastDispatched) {
                exportTasks.add(
                        dispatchCurrentBatch(nextLevelNodes.subList(lastDispatched, nodeCounter), level));
                log.info("Tasks submitted: " + exportTasks.size());
            }

            waitForLevelToFinish(level, exportTasks);
            levels.set(level, null); // try to save some memory
            if (nextLevelNodes.isEmpty()) {
                log.info("No more levels to process, last level was: " + level);
                break;
            }
        }
        log.info(String.format("Level-wise export finished, elapsed time %s seconds", (System.currentTimeMillis() - startTime) / 1000));
    }

    private void waitForLevelToFinish(int level, List<Future> exportTasks) {
        if (exportTasks.size() > 0) {
            for (int i = 1; i <= exportTasks.size(); i++) {
                try {
                    completionService.take().get();
                    if (log.isDebugEnabled()) {
                        log.debug("Tasks completed: " + i + " out of " + exportTasks.size());
                    }
                    else {
                        if (i % 100 == 0){
                            log.info("Tasks completed: " + i + " out of " + exportTasks.size() + " level " + level);
                        }
                    }
                } catch (ExecutionException | InterruptedException e) {
                    log.error("Export processing was interrupted");
                    throw new RuntimeException(e);
                }
            }
            closeCsvWriters(level);
            closeProtoStreams(level);
            log.info("Level " + level + " export is finished");
        }
    }

    private void closeProtoStreams(int level) {
        for (Map.Entry<String, BufferedOutputStream> protoStream : levelProtoStreams.get(level).entrySet()){
            try {
                protoStream.getValue().close();
            } catch (IOException e){
                log.error("Failed to close protoStream " + protoStream.getKey() + " for level " + level);
            }
        }
        levelProtoStreams.get(level).clear();
    }

    private void closeCsvWriters(int level) {
        for (Map.Entry<String, BufferedWriter> threadWriter : levelThreadWriters.get(level).entrySet()) {
            try {
                threadWriter.getValue().close();
            } catch (IOException e) {
                log.error("Failed to close threadWriter " + threadWriter.getKey() + " for level " + level);
            }
        }
        levelThreadWriters.get(level).clear();
    }

    private Future<?> dispatchCurrentBatch(List<ExportNode> exportNodes, int level) {
        ExportTask exportTask = new ExportTask(exportNodes, level);
        return completionService.submit(exportTask, 1);
    }


    private List<ExportNode> getStartNodes() {
        List<ExportNode> startNodes = new ArrayList<>();
        try (BufferedReader bufferedReader =
                     new BufferedReader(new FileReader(rootNodeListInput))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                ExportNode startNode = new ExportNode();
                startNode.nodeRef = new NodeRef(StoreRef.STORE_REF_WORKSPACE_SPACESSTORE, line);
                startNode.name = (String) nodeService.getProperty(startNode.nodeRef, ContentModel.PROP_NAME);
                startNode.fullPath = "/" + startNode.name;
                startNode.isFolder = true;
                startNodes.add(startNode);
            }
        } catch (IOException e) {
            log.error("Failed to read the root node list", e);
        } catch (Exception e) {
            log.error("Failed constructing start nodes array", e);
        }
        return startNodes;
    }

    private class ExportTask implements Runnable {
        private Log log = LogFactory.getLog(ExportTask.class);
        private List<ExportNode> exportNodes;
        private Integer level;
        private BufferedWriter threadWriter;
        private BufferedOutputStream protoStream;

        public ExportTask(List<ExportNode> exportNodes, Integer level) {
            this.exportNodes = exportNodes;
            this.level = level;
        }

        @Override
        public void run() {
            initThreadWriter();
            try {
                initProtoStream();
            } catch (IOException e) {
                log.error("Failed to initialize protobuf output stream", e);
                throw new RuntimeException(e);
            }
            AuthenticationUtil.setAdminUserAsFullyAuthenticatedUser();
            for (ExportNode exportNode : exportNodes) {
                //exportMetadataToFileStructure(exportNode);
                try {
                    protoExportService.exportNodeToFile(exportNode, protoStream);
                } catch (IOException e) {
                    log.error("Failed to export node to protobuf " + exportNode.fullPath, e);
                }
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

        private void exportMetadataToFileStructure(ExportNode node) {
            wirelineExportService.createDirectory(node, datedExportFolder);
            try {
                wirelineExportService.writePropertyAndTranslationsFile(node, datedExportFolder);
                wirelineExportService.writeAclFile(node, datedExportFolder);
            } catch (Exception e) {
                log.error("Error by processing node " + node.fullPath);
                throw new RuntimeException(e);
            }
        }

        private void initThreadWriter() {
            String threadName = Thread.currentThread().getName();
            Map<String, BufferedWriter> threadWriters;
            if (!levelThreadWriters.containsKey(level)) {
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

        private void initProtoStream() throws IOException {
            String threadName = Thread.currentThread().getName();
            if (!levelProtoStreams.containsKey(level)) {
                levelProtoStreams.put(level, new HashMap<String, BufferedOutputStream>());
            }
            Map<String, BufferedOutputStream> protoStreams = levelProtoStreams.get(level);
            if (!protoStreams.containsKey(threadName)) {
                Path protoOutputFile = datedExportFolder.resolve("protobuf").resolve(String.valueOf(level))
                        .resolve(threadName + ".proto");
                Files.createDirectories(protoOutputFile.getParent());
                BufferedOutputStream protoStream = new BufferedOutputStream(
                        new FileOutputStream(protoOutputFile.toFile()));
                protoStreams.put(threadName, protoStream);
            }
            this.protoStream = protoStreams.get(threadName);
        }
    }
}
