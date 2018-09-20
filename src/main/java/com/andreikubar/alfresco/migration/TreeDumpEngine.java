package com.andreikubar.alfresco.migration;

import com.andreikubar.alfresco.migration.export.ExportNode;
import com.andreikubar.alfresco.migration.export.ExportNodeBuilder;
import com.andreikubar.alfresco.migration.export.wireline.WirelineExportService;
import de.siegmar.fastcsv.reader.CsvContainer;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRow;
import org.alfresco.model.ContentModel;
import org.alfresco.query.PagingRequest;
import org.alfresco.query.PagingResults;
import org.alfresco.repo.content.ContentStore;
import org.alfresco.repo.node.MLPropertyInterceptor;
import org.alfresco.repo.security.authentication.AuthenticationUtil;
import org.alfresco.service.ServiceRegistry;
import org.alfresco.service.cmr.model.FileFolderService;
import org.alfresco.service.cmr.model.FileInfo;
import org.alfresco.service.cmr.repository.*;
import org.alfresco.service.namespace.NamespaceService;
import org.alfresco.service.namespace.QName;
import org.alfresco.service.namespace.RegexQNamePattern;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TreeDumpEngine {
    public static final String PROPERTY_FULL_PATH_AT_SOURCE = "fullPathAtSource";
    public static final String PROPERTY_EXPORT_ID = "exportId";
    public static final int MAX_ITEMS_PER_PAGE = 10;

    private enum ChildQueryMode {WITH_PAGING, NO_PAGING}

    ;
    private Log log = LogFactory.getLog(TreeDumpEngine.class);

    private String csvOutputDir;
    private String rootNodeListInput;
    private Boolean checkPhysicalExistence;
    private String contentStoreBase;
    private String propertyFile;
    private Boolean exportMetadata;
    private String exportDestination;
    private Boolean useCmName;
    private int foldersProBatch;
    private ChildQueryMode childQueryMode;

    private ExportNodeBuilder exportNodeBuilder;
    private NodeService nodeService;
    private NamespaceService namespaceService;
    private ContentStore defaultContentStore;
    private FileFolderService fileFolderService;
    private WirelineExportService wirelineExportService;
    private TreeDumperTask dumpTreeTask;
    private Map<String, BufferedWriter> threadWriters;
    private Path datedExportFolder;
    private String currentNodeId;
    private Path currentExportBasePath;
    private Map<String, Map<String, String>> startNodesInputMap;
    private AtomicInteger totalChildrenFound = new AtomicInteger();
    private AtomicInteger totalExistenceCheckFailed = new AtomicInteger();
    private boolean cancelled;

    private ForkJoinPool forkJoinPool;

    public TreeDumpEngine(ServiceRegistry serviceRegistry) {
        nodeService = serviceRegistry.getNodeService();
        namespaceService = serviceRegistry.getNamespaceService();
        this.fileFolderService = serviceRegistry.getFileFolderService();
        if (checkPhysicalExistence == null) checkPhysicalExistence = true;
    }

    public void setPropertyFile(String propertyFile) {
        this.propertyFile = propertyFile;
    }

    public void setWirelineExportService(WirelineExportService wirelineExportService) {
        this.wirelineExportService = wirelineExportService;
    }

    public void setDefaultContentStore(ContentStore defaultContentStore) {
        this.defaultContentStore = defaultContentStore;
    }

    private void loadPropeties() {
        Properties properties = new Properties();
        InputStream input;
        try {
            input = new FileInputStream(this.propertyFile);
            properties.load(input);

            if (StringUtils.isBlank(this.csvOutputDir = properties.getProperty("csvOutputDir"))) {
                throw new IllegalArgumentException("parameter csvOutputDir is missing");
            }
            if (StringUtils.isBlank(this.rootNodeListInput = properties.getProperty("rootNodeListInput"))) {
                throw new IllegalArgumentException("parameter rootNodeListInput is missing");
            }

            this.exportMetadata = Boolean.valueOf(properties.getProperty("exportMetadata"));
            this.exportDestination = properties.getProperty("exportDestination");
            if (this.exportMetadata && StringUtils.isBlank(this.exportDestination)) {
                throw new IllegalArgumentException("parameter exportDestination is missing");
            }

            try {
                this.foldersProBatch = Integer.parseInt(properties.getProperty("foldersProBatch"));
            } catch (NumberFormatException e) {
                this.foldersProBatch = 5;
            }

            this.checkPhysicalExistence = Boolean.valueOf(properties.getProperty("checkPhysicalExistence"));
            this.useCmName = Boolean.valueOf(properties.getProperty("useCmName"));
            if (properties.getProperty("childQueryMode").equalsIgnoreCase(ChildQueryMode.WITH_PAGING.name())) {
                this.childQueryMode = ChildQueryMode.WITH_PAGING;
            } else {
                this.childQueryMode = ChildQueryMode.NO_PAGING;
            }


        } catch (FileNotFoundException e) {
            log.error("Property file not found " + this.propertyFile);
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void setExportNodeBuilder(ExportNodeBuilder exportNodeBuilder) {
        this.exportNodeBuilder = exportNodeBuilder;
    }

    void startAndWait() {
        if (forkJoinPool == null || forkJoinPool.isTerminated()) {
            forkJoinPool = new ForkJoinPool();
        }
        else if (forkJoinPool.isTerminating()){
            log.error("ForkJoin pool is still terminating");
            log.error("Active threads: " + forkJoinPool.getActiveThreadCount());
            throw new RuntimeException("ForkJoin Pool is still terminating");
        }
        cancelled = false;
        totalChildrenFound.set(0);
        AuthenticationUtil.setAdminUserAsFullyAuthenticatedUser();
        loadPropeties();
        createOrCleanCsvOutDir();
        this.contentStoreBase = defaultContentStore.getRootLocation();
        this.exportNodeBuilder.setUseCmName(this.useCmName);
        if (this.exportMetadata) {
            this.exportNodeBuilder.setReadProperties(true);
        }



        threadWriters = new HashMap<>();

        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd-HHmm");
        Date date = new Date();
        String datedSubfolderName = format.format(date);
        this.datedExportFolder = Paths.get(exportDestination).resolve(datedSubfolderName);


        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
        String startDateTime = sdf.format(cal.getTime());
        long startTime = System.currentTimeMillis();
        List<ExportNode> startNodes = readStartNodes();

        for (ExportNode startNode : startNodes) {
            String startNodeFullPath = startNodesInputMap.get(startNode.nodeRef.getId()).get(PROPERTY_FULL_PATH_AT_SOURCE);
            currentNodeId = startNode.nodeRef.getId();
            List<ExportNode> exportNodeSingleList = Collections.singletonList(startNode);
            String nodeExportId = startNodesInputMap.get(startNode.nodeRef.getId()).get(PROPERTY_EXPORT_ID);
            currentExportBasePath = datedExportFolder.resolve(nodeExportId);

            log.info("******************************************");
            log.info(String.format("TreeDump starting - %s", startDateTime));
            log.info(String.format("             Node to export: %s (%s)", startNodeFullPath, startNode.nodeRef));
            log.info(String.format("    Check existence on disk: %s", checkPhysicalExistence));
            log.info(String.format("         Content store base: %s", contentStoreBase));
            log.info(String.format("              Get cm:name's: %s", exportNodeBuilder.getUseCmName()));
            log.info(String.format("       Root nodes read from: %s", rootNodeListInput));
            log.info(String.format("         Results written to: %s", csvOutputDir));
            log.info(String.format("         Metadata export is: %s", exportMetadata ? "ON" : "OFF"));
            log.info(String.format("                  Paging is: %s", childQueryMode == ChildQueryMode.WITH_PAGING ? "ON" : "OFF"));
            if (exportMetadata) {
                log.info(String.format("Metadata export destination: %s", currentExportBasePath.toString()));
            }

            if (exportMetadata) {
                exportMetadataToFileStructure(startNode);
            }
            dumpTreeTask = new TreeDumperTask(exportNodeSingleList, 0);

            forkJoinPool.execute(dumpTreeTask);
            long taskStartTime = System.currentTimeMillis();
            do {
                log.info("******************************************");
                log.info(String.format("Main: Parallelism: %d", forkJoinPool.getParallelism()));
                log.info(String.format("Main: Active Threads: %d", forkJoinPool.getActiveThreadCount()));
                log.info(String.format("Main: Task Count: %d", forkJoinPool.getQueuedTaskCount()));
                log.info(String.format("Main: Steal Count: %d", forkJoinPool.getStealCount()));
                log.info(String.format("Total children found: %d", totalChildrenFound.get()));
                log.info(String.format("Total files failed existence check: %d", totalExistenceCheckFailed.get()));
                log.info("******************************************");
                try {
                    TimeUnit.SECONDS.sleep(5);
                } catch (InterruptedException e) {
                    log.error("Execution interrupted", e);
                }
            } while (!dumpTreeTask.isDone());

            addDoneSuffixToExportFolder();

            log.info(String.format("TreeDump finished for node %s", startNodeFullPath));
            log.info(String.format("TreeDump elapsed time - %s seconds", (System.currentTimeMillis() - taskStartTime) / 1000));

            flushThreadWriters();
        }

        closeThreadWriters();

        log.info("TreeDump finished");
        log.info(String.format("TreeDump elapsed time - %s seconds", (System.currentTimeMillis() - startTime) / 1000));
        log.info(String.format("Total children processed: %d", totalChildrenFound.get()));
        log.info(String.format("Total files failed existence check: %d", totalExistenceCheckFailed.get()));
        log.info("******************************************");
    }

    private void addDoneSuffixToExportFolder() {

        Path exportDonePath = currentExportBasePath.getParent().resolve(
                currentExportBasePath.getFileName().toString() + ".done");
        try {
            Files.move(currentExportBasePath, exportDonePath);
        } catch (IOException e) {
            log.error("Failed to rename the export folder to done " + currentExportBasePath.toString());
        }
    }

    private void closeThreadWriters() {
        for (BufferedWriter writer : threadWriters.values()) {
            try {
                writer.close();
            } catch (IOException e) {
                log.error("Failed to close BufferedWriter", e);
            }
        }
        threadWriters.clear();
    }

    private void flushThreadWriters() {
        for (BufferedWriter writer : threadWriters.values()) {
            try {
                writer.flush();
            } catch (IOException e) {
                log.error("Failed to flush BufferedWriter", e);
            }
        }
    }

    private List<ExportNode> readStartNodes() {
        CsvReader csvReader = new CsvReader();
        csvReader.setContainsHeader(false);
        List<ExportNode> startNodes = new ArrayList<>();
        startNodesInputMap = new HashMap<>();
        CsvContainer startNodesContainer;
        try {
            startNodesContainer = csvReader.read(Paths.get(rootNodeListInput), Charset.forName("UTF-8"));
        } catch (IOException e) {
            log.error("Failed to read the rootnodes csv file " + rootNodeListInput);
            throw new RuntimeException(e);
        }
        for (CsvRow row : startNodesContainer.getRows()) {
            NodeRef nodeRef = new NodeRef(StoreRef.STORE_REF_WORKSPACE_SPACESSTORE, row.getField(1));
            String fullPath = row.getField(2);
            ExportNode startNode = exportNodeBuilder.constructExportNode(nodeRef);
            startNodes.add(startNode);
            Map<String, String> startNodeProperties = new HashMap<>();
            startNodeProperties.put(PROPERTY_EXPORT_ID, row.getField(0));
            startNodeProperties.put(PROPERTY_FULL_PATH_AT_SOURCE, fullPath);
            startNodesInputMap.put(nodeRef.getId(), startNodeProperties);
        }
        return startNodes;
    }

    void shutdown() {
        cancelled = true;
        this.dumpTreeTask.cancel(true);
        this.forkJoinPool.shutdownNow();
        log.info("TreeDump shutdown");
    }

    private void createOrCleanCsvOutDir() {
        Path csvOutPath = Paths.get(csvOutputDir);
        if (!Files.exists(csvOutPath)) {
            try {
                Files.createDirectories(csvOutPath);
            } catch (IOException e) {
                log.error("Failed to create output directory", e);
            }
        } else {
            try {
                FileUtils.cleanDirectory(Paths.get(csvOutputDir).toFile());
            } catch (IOException e) {
                log.warn("Failed to clean the csv output directory");
            }
        }
    }


    private class TreeDumperTask extends RecursiveAction {
        private List<ExportNode> parentNodes;
        private List<ExportNode> subFolders = new ArrayList<>(50);

        private int recursionLevel;

        TreeDumperTask(List<ExportNode> parentNodes, int recursionLevel) {
            this.parentNodes = parentNodes;
            this.recursionLevel = recursionLevel;
        }

        @Override
        public void compute() {
            if (!threadWriters.containsKey(Thread.currentThread().getName())) {
                try {
                    Path csvOutFile = Paths.get(csvOutputDir,
                            String.format("%s-%s.csv", "treedump", Thread.currentThread().getName()));
                    BufferedWriter csvWriter = new BufferedWriter(
                            new OutputStreamWriter(
                                    new FileOutputStream(csvOutFile.toFile(), true), StandardCharsets.UTF_8));
                    threadWriters.put(Thread.currentThread().getName(), csvWriter);
                } catch (FileNotFoundException e) {
                    log.error("Failed to open BufferedWriter", e);
                }
            }

            AuthenticationUtil.setAdminUserAsFullyAuthenticatedUser();

            for (ExportNode parentNode : parentNodes) {
                if (cancelled){
                    throw new RuntimeException("Task cancelled");
                }
                try {
                    if (childQueryMode == ChildQueryMode.WITH_PAGING) {
                        processChildNodesWithPaging(parentNode);
                    } else {
                        processChildNodes(parentNode);
                    }
                } catch (Exception e) {
                    log.error("Failed to process children of " + parentNode.fullPath, e);
                }
            }
            try {
                processSubFoldersRecursively();
            } catch (Exception e) {
                log.error("Failed to process recursive tasks for subfolders", e);
            }
        }

        private void processSubFoldersRecursively() {
            if (cancelled){
                return;
            }
            if (subFolders.size() > 0) {
                int batch_size = foldersProBatch;
                if (subFolders.size() >= batch_size) {
                    int number_of_batches = (int) Math.floor(subFolders.size() / batch_size);
                    List<RecursiveAction> actionsToFork = new ArrayList<>(number_of_batches);
                    int lower_bound;
                    int upper_bound = 0;
                    int batch_index;
                    for (batch_index = 0; batch_index < number_of_batches; batch_index++) {
                        lower_bound = batch_index * batch_size;
                        upper_bound = (batch_index + 1) * batch_size;
                        actionsToFork.add(
                                new TreeDumperTask(subFolders.subList(lower_bound, upper_bound), recursionLevel + 1)
                        );
                    }
                    if (subFolders.size() > upper_bound) { // pack the rests into one last Task
                        actionsToFork.add(
                                new TreeDumperTask(subFolders.subList(upper_bound, subFolders.size()), recursionLevel + 1)
                        );
                    }
                    invokeAll(actionsToFork); // fork all tasks and wait for completion
                } else {
                    (new TreeDumperTask(subFolders, recursionLevel + 1)).compute();
                }
                subFolders.clear();
            }

        }

        private void processChildNodesWithPaging(ExportNode parentNode) {
            PagingRequest pagingRequest;
            MLPropertyInterceptor.setMLAware(true);
            int pageCount = 0;
            boolean hasMoreItems;
            String queryId = null;
            do {
                if (cancelled) {
                    break;
                }
                if (queryId != null) {
                    pagingRequest = new PagingRequest(pageCount * MAX_ITEMS_PER_PAGE, MAX_ITEMS_PER_PAGE, queryId);
                } else {
                    pagingRequest = new PagingRequest(pageCount * MAX_ITEMS_PER_PAGE, MAX_ITEMS_PER_PAGE);
                }
                PagingResults<FileInfo> results = fileFolderService.list(
                        parentNode.nodeRef, true, true, null, null, pagingRequest
                );
                totalChildrenFound.getAndAdd(results.getPage().size());
                for (FileInfo fileInfo : results.getPage()) {
                    if (cancelled) {
                        break;
                    }
                    try {
                        ExportNode childNode = exportNodeBuilder.constructExportNode(fileInfo, parentNode.fullPath);
                        if (childNode.isFolder) {
                            subFolders.add(childNode);
                        }
                        dumpNode(parentNode, childNode);
                    } catch (Exception e) {
                        log.error("Failed to dump child node " + parentNode.fullPath + "/" + fileInfo.getName(), e);
                    }
                }
                hasMoreItems = results.hasMoreItems();
                queryId = results.getQueryExecutionId();
                pageCount++;
            } while (hasMoreItems);
            MLPropertyInterceptor.setMLAware(false);
        }

        private void processChildNodes(ExportNode parentNode) {
            List<ChildAssociationRef> childAssocs =
                    nodeService.getChildAssocs(parentNode.nodeRef, ContentModel.ASSOC_CONTAINS, RegexQNamePattern.MATCH_ALL);
            if (childAssocs.size() == 0) {
                return;
            }
            if (childAssocs.size() > 500) {
                log.info(String.format("Found %d children for parent node %s", childAssocs.size(), parentNode.fullPath));
            }
            totalChildrenFound.getAndAdd(childAssocs.size());

            for (ChildAssociationRef assoc : childAssocs) {
                if (cancelled) {
                    break;
                }
                try {
                    ExportNode childNode = exportNodeBuilder.constructExportNode(assoc, parentNode.fullPath);
                    if (childNode.isFolder) {
                        subFolders.add(childNode);
                    }
                    dumpNode(parentNode, childNode);
                } catch (Exception e) {
                    log.error("Failed to process child " + assoc.getQName().toPrefixString(namespaceService), e);
                }
            }
        }

        private void dumpNode(ExportNode parentNode, ExportNode childNode) throws IOException {
            if (childNode.isFile && checkPhysicalExistence) {
                if (!existsInContentStore(childNode.nodeRef)) {
                    totalExistenceCheckFailed.incrementAndGet();
                    log.debug("Content store check failed for " + childNode.fullPath + " " + childNode.nodeRef);
                    return;
                }
            }
            if (exportMetadata) {
                exportMetadataToFileStructure(childNode);
            }
            writeCsvLine(
                    threadWriters.get(Thread.currentThread().getName()),
                    parentNode.nodeRef.getId(),
                    childNode.nodeRef.getId(),
                    childNode.name,
                    childNode.nodeType,
                    recursionLevel,
                    childNode.isFolder,
                    parentNode.fullPath,
                    childNode.contentUrl,
                    childNode.contentBytes);
        }
    }

    private boolean existsInContentStore(NodeRef childRef) {
        ContentData contentData = (ContentData) nodeService.getProperty(childRef,
                QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "content"));
        if (contentData == null) {
            log.warn("Type is Content but ContentData is null");
            return false;
        }
        String contentURL = contentData.getContentUrl();
        if (contentURL == null) {
            log.warn("Type is Content but ContentUrl is null");
            return false;
        }
        String contentLocationWithinStore = contentURL.replace("store://", "");
        Path pathToCheck = Paths.get(contentStoreBase, contentLocationWithinStore);
        boolean fileExists = Files.exists(pathToCheck);
        if (!fileExists) log.debug(String.format("existence check FAILED for: %s", pathToCheck.toString()));
        return fileExists;
    }

    private void writeCsvLine(BufferedWriter csvWriter, String parentId, String childId, String nodeName,
                              QName childNodeType, int level, boolean isFolder, String containingPath,
                              String contentUrl, long contentBytes) throws IOException {
        String csvLine = String.format("%d|%s|%s|%s|%s|%s|%s|%s|%d\n",
                level,
                parentId,
                childId,
                childNodeType.getPrefixedQName(namespaceService).getPrefixString(),
                isFolder ? "isFolder" : "notAFolder",
                containingPath,
                nodeName,
                contentUrl,
                contentBytes
        );
        csvWriter.append(csvLine);
    }

    private void exportMetadataToFileStructure(ExportNode node) {
        createDirectory(node, currentExportBasePath);
        try {
            wirelineExportService.writePropertyAndTranslationsFile(node, currentExportBasePath);
            wirelineExportService.writeAclFile(node, currentExportBasePath);
        } catch (Exception e) {
            log.error("Error by processing node " + node.fullPath);
            throw new RuntimeException(e);
        }
    }

    private void createDirectory(ExportNode node, Path exportBasePath) {
        try {
            Path pathToCreate = node.isFolder ? Paths.get(node.fullPath) : Paths.get(node.fullPath).getParent();
            if (pathToCreate.getRoot() != null) {
                pathToCreate = pathToCreate.subpath(0, pathToCreate.getNameCount());
            }
            Files.createDirectories(exportBasePath.resolve(pathToCreate));
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }

}

