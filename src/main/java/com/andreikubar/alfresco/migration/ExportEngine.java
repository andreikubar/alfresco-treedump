package com.andreikubar.alfresco.migration;

import com.andreikubar.alfresco.migration.export.ExportNode;
import com.andreikubar.alfresco.migration.export.ExportNodeBuilder;
import com.andreikubar.alfresco.migration.export.NodeRefExt;
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
import java.util.concurrent.atomic.AtomicInteger;

public class ExportEngine {
    public static final String DEFAULT_BATCH_SIZE = "50";
    public static final String DEFAULT_SCOUT_THREADS = "2";
    public static CsvOutput csvOutput;
    private Log log = LogFactory.getLog(ExportEngine.class);

    private String rootNodeListInput;
    private String propertyFile;
    private String exportDestination;
    private String csvOutputDir;
    private int scoutThreads;
    private int dumperThreads;
    private int dumpBatchSize;

    private NodeService nodeService;
    private NamespaceService namespaceService;
    private ContentStore defaultContentStore;
    private WirelineExportService wirelineExportService;
    private ProtoExportService protoExportService;
    private ExportNodeBuilder exportNodeBuilder;

    private ExecutorService dumperService;
    private ExecutorService scoutService;
    private CompletionService<Integer> dumperCompletionService;
    private CompletionService<Integer> scoutCompletionService;
    private Map<Integer, Map<String, BufferedWriter>> levelThreadWriters = new HashMap<>();
    private Map<Integer, Map<String, BufferedOutputStream>> levelProtoStreams = new HashMap<>();
    private Path datedExportFolder;

    private AtomicInteger tasksSubmitted;
    private AtomicInteger tasksFinished;
    private int currentLevel;

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
            this.scoutThreads = Integer.parseInt(properties.getProperty("scoutThreads", DEFAULT_SCOUT_THREADS));
            this.dumperThreads = Integer.parseInt(properties.getProperty("dumperThreads",
                    String.valueOf(Runtime.getRuntime().availableProcessors())));
            this.dumpBatchSize = Integer.parseInt(properties.getProperty("dumpBatchSize", DEFAULT_BATCH_SIZE));
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
        if (dumperService == null || dumperService.isShutdown()) {
            dumperService = Executors.newFixedThreadPool(dumperThreads);
        }
        if (scoutService == null || scoutService.isShutdown()) {
            scoutService = Executors.newFixedThreadPool(scoutThreads);
        }
        dumperCompletionService = new ExecutorCompletionService<>(dumperService);
        scoutCompletionService = new ExecutorCompletionService<>(scoutService);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HHmm");
        Date date = new Date();
        String datedSubfolderName = format.format(date);
        this.datedExportFolder = Paths.get(exportDestination).resolve(datedSubfolderName);

        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
        String startDateTime = sdf.format(cal.getTime());
        long startTime = System.currentTimeMillis();
        log.info(String.format("Level-wise export starting - %s", startDateTime));
        log.info(String.format("              Get cm:name's: %s", exportNodeBuilder.getUseCmName()));
        log.info(String.format("       Root nodes read from: %s", rootNodeListInput));
        log.info(String.format("         Results written to: %s", csvOutputDir));
        log.info(String.format("   Number of dumper threads: %d", dumperThreads));
        log.info(String.format("    Number of scout threads: %d", scoutThreads));

        startExportMonitor();

        List<NodeRefExt> startNodes = getStartNodes();
        List<List<NodeRefExt>> levels = new ArrayList<>();
        levels.add(startNodes);
        List<NodeRefExt> nextLevelNodes;
        for (int level = 0; ; level++) {
            log.info("Starting level " + level + " export");
            currentLevel = level;
            tasksFinished = new AtomicInteger();
            tasksSubmitted = new AtomicInteger();
            nextLevelNodes = Collections.synchronizedList(new ArrayList<NodeRefExt>());
            levels.add(nextLevelNodes);

            List<NodeRefExt> currentLevelNodes = levels.get(level);
            int nodesProBatch = (int) Math.ceil((double) currentLevelNodes.size() / scoutThreads);
            int lower_bound = 0;
            int upper_bound = 0;
            int scout_tasks_submitted = 0;
            for (int i = 0; i < scoutThreads; i++) {
                upper_bound = (i + 1) * nodesProBatch;
                if (upper_bound >= currentLevelNodes.size()) {
                    scoutCompletionService.submit(new ScoutTask(
                            currentLevelNodes.subList(lower_bound, currentLevelNodes.size()), nextLevelNodes), 1);
                    scout_tasks_submitted++;
                    break;
                } else {
                    scoutCompletionService.submit(new ScoutTask(
                            currentLevelNodes.subList(lower_bound, upper_bound), nextLevelNodes), 1);
                    scout_tasks_submitted++;
                    lower_bound = upper_bound;
                }
            }

            log.debug("Scout tasks submitted " + scout_tasks_submitted);
            waitForScoutingToFinish(scout_tasks_submitted);
            waitForLevelExportToFinish();
            levels.set(level, null); // try to save some memory
            if (nextLevelNodes.isEmpty()) {
                log.info("No more levels to process, last level was: " + level);
                break;
            }
        }
        dumperService.shutdown();
        scoutService.shutdown();
        log.info(String.format("Level-wise export finished, elapsed time %s seconds", (System.currentTimeMillis() - startTime) / 1000));
    }

    private void startExportMonitor() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (!dumperService.isTerminated()) {
                    log.info(String.format("Current level: %d Tasks submitted: %d Tasks completed: %d",
                            currentLevel, tasksSubmitted, tasksFinished));
                    try {
                        TimeUnit.SECONDS.sleep(7);
                    } catch (InterruptedException e) {
                        log.error("Task monitor interrupted");
                    }
                }
            }
        }).start();
    }

    private void waitForScoutingToFinish(int scout_tasks_submitted) {
        if (scout_tasks_submitted > 0) {
            for (int i = 0; i < scout_tasks_submitted; i++) {
                try {
                    scoutCompletionService.take();
                } catch (InterruptedException e) {
                    log.error("Scouting interrupted", e);
                }
            }
        }
    }

    private void waitForLevelExportToFinish() {
        if (tasksSubmitted.get() > 0) {
            for (int i = 1; i <= tasksSubmitted.get(); i++) {
                try {
                    dumperCompletionService.take().get();
                    log.debug("Tasks completed: " + i + " out of " + tasksSubmitted.get());
                } catch (ExecutionException | InterruptedException e) {
                    log.error("Export processing was interrupted");
                    throw new RuntimeException(e);
                }
            }
            closeCsvWriters(currentLevel);
            closeProtoStreams(currentLevel);
            log.info("Level " + currentLevel + " export is finished");
        }
    }

    private void closeProtoStreams(int level) {
        for (Map.Entry<String, BufferedOutputStream> protoStream : levelProtoStreams.get(level).entrySet()) {
            try {
                protoStream.getValue().close();
            } catch (IOException e) {
                log.error("Failed to close protoStream " + protoStream.getKey() + " for level " + level);
            }
        }
        levelProtoStreams.get(level).clear();
    }

    private void flushProtoStreams(int level) {
        for (Map.Entry<String, BufferedOutputStream> protoStream : levelProtoStreams.get(level).entrySet()) {
            try {
                protoStream.getValue().flush();
            } catch (IOException e) {
                log.error("Failed to flush protoStream " + protoStream.getKey() + " for level " + level);
            }
        }
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

    private void flushCsvWriters(int level) {
        for (Map.Entry<String, BufferedWriter> threadWriter : levelThreadWriters.get(level).entrySet()) {
            try {
                threadWriter.getValue().flush();
            } catch (IOException e) {
                log.error("Failed to flush threadWriter " + threadWriter.getKey() + " for level " + level);
            }
        }
    }

    private List<NodeRefExt> getStartNodes() {
        List<NodeRefExt> startNodes = new ArrayList<>();
        try (BufferedReader bufferedReader =
                     new BufferedReader(new FileReader(rootNodeListInput))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                NodeRef nodeRef = new NodeRef(StoreRef.STORE_REF_WORKSPACE_SPACESSTORE, line);
                String fullPath = "/" + (String) nodeService.getProperty(nodeRef, ContentModel.PROP_NAME);
                NodeRefExt startNode = new NodeRefExt(nodeRef, fullPath);
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
                } catch (Exception e) {
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
                } catch (Exception e) {
                    log.error("Failed to write CSV line for child " + exportNode.fullPath);
                }
            }
            tasksFinished.incrementAndGet();
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

    private class ScoutTask implements Runnable {

        List<NodeRefExt> parentNodes;
        List<NodeRefExt> nextLevelNodes = new ArrayList<>();
        List<NodeRefExt> globalNextLevelList;
        List<ExportNode> nodesToDump = new ArrayList<>();

        public ScoutTask(List<NodeRefExt> parentNodes, List<NodeRefExt> globalNextLevelList) {
            this.parentNodes = parentNodes;
            this.globalNextLevelList = globalNextLevelList;
        }

        @Override
        public void run() {
            try {
                AuthenticationUtil.setAdminUserAsFullyAuthenticatedUser();
                int nodeCounter = 0;
                int lastDispatched = 0;
                int var10;
                for (NodeRefExt parentNode : parentNodes) {
                    try {
                        List<ChildAssociationRef> childAssocs = nodeService.getChildAssocs(parentNode.nodeRef,
                                ContentModel.ASSOC_CONTAINS, RegexQNamePattern.MATCH_ALL);
                        if (childAssocs.size() > 0) {
                            for (ChildAssociationRef childAssoc : childAssocs) {
                                ExportNode exportNode = exportNodeBuilder.constructExportNode(childAssoc, parentNode.fullPath);
                                if (exportNode.isFolder) {
                                    nextLevelNodes.add(new NodeRefExt(exportNode.nodeRef, exportNode.fullPath));
                                }
                                nodesToDump.add(exportNode);
                                nodeCounter++;
                                if (nodeCounter - lastDispatched >= dumpBatchSize) {
                                    dispatchCurrentBatch(
                                            new ArrayList<>(nodesToDump.subList(lastDispatched, nodeCounter)), currentLevel);
                                    var10 = tasksSubmitted.incrementAndGet();
                                    log.debug("Tasks submitted: " + var10);
                                    lastDispatched = nodeCounter;
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.error("Failed to scout parent node " + parentNode.fullPath + " " + parentNode.nodeRef, e);
                    }
                }
                if (nodeCounter > lastDispatched) {
                    dispatchCurrentBatch(nodesToDump.subList(lastDispatched, nodeCounter), currentLevel);
                    var10 = tasksSubmitted.incrementAndGet();
                    log.info("Tasks submitted: " + var10);
                }
                globalNextLevelList.addAll(nextLevelNodes);
            } catch (Exception e){
                log.error("Error in scout task", e);
                throw e;
            }
        }

        private void dispatchCurrentBatch(List<ExportNode> exportNodes, int level) {
            ExportTask exportTask = new ExportTask(exportNodes, level);
            dumperCompletionService.submit(exportTask, 1);
        }
    }

}
