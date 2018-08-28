package com.andreikubar.alfresco.migration;

import com.andreikubar.alfresco.migration.export.ExportNode;
import com.andreikubar.alfresco.migration.export.ExportNodeBuilder;
import com.andreikubar.alfresco.migration.export.wireline.WirelineExportService;
import org.alfresco.model.ContentModel;
import org.alfresco.repo.content.ContentStore;
import org.alfresco.repo.security.authentication.AuthenticationUtil;
import org.alfresco.service.ServiceRegistry;
import org.alfresco.service.cmr.repository.*;
import org.alfresco.service.namespace.NamespaceService;
import org.alfresco.service.namespace.QName;
import org.alfresco.service.namespace.RegexQNamePattern;
import org.apache.commons.io.FileUtils;
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
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;

public class TreeDumpEngine {
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

    private ExportNodeBuilder exportNodeBuilder;
    private NodeService nodeService;
    private NamespaceService namespaceService;
    private ContentStore defaultContentStore;
    private WirelineExportService wirelineExportService;
    private TreeDumperTask dumpTreeTask;
    private Map<String, BufferedWriter> threadWriters;
    private Path datedExportFolder;

    private ForkJoinPool forkJoinPool;

    public TreeDumpEngine(ServiceRegistry serviceRegistry) {
        nodeService = serviceRegistry.getNodeService();
        namespaceService = serviceRegistry.getNamespaceService();
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

            this.exportMetadata = Boolean.valueOf(properties.getProperty("exportMetadata"));
            if (this.exportMetadata && StringUtils.isBlank(this.exportDestination = properties.getProperty("exportDestination"))) {
                throw new IllegalArgumentException("parameter exportDestination is missing");
            }

            try {
                this.foldersProBatch = Integer.parseInt(properties.getProperty("foldersProBatch"));
            } catch (NumberFormatException e) {
                this.foldersProBatch = 5;
            }

            this.checkPhysicalExistence = Boolean.valueOf(properties.getProperty("checkPhysicalExistence"));
            this.useCmName = Boolean.valueOf(properties.getProperty("useCmName"));


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
        loadPropeties();
        createOrCleanCsvOutDir();
        this.contentStoreBase = defaultContentStore.getRootLocation();
        this.exportNodeBuilder.setUseCmName(this.useCmName);
        if (this.exportMetadata) {
            this.exportNodeBuilder.setReadProperties(true);
        }

        AuthenticationUtil.setAdminUserAsFullyAuthenticatedUser();

        List<ExportNode> startNodes = new ArrayList<>();
        try (BufferedReader bufferedReader =
                     new BufferedReader(new FileReader(rootNodeListInput))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                ExportNode startNode = new ExportNode();
                startNode.nodeRef = new NodeRef(StoreRef.STORE_REF_WORKSPACE_SPACESSTORE, line);
                startNode.name = (String) nodeService.getProperty(startNode.nodeRef, ContentModel.PROP_NAME);
                startNode.fullPath = "/" + startNode.name;
                startNodes.add(startNode);
            }
        } catch (IOException e) {
            log.error("Failed to read the root node list", e);
            return;
        } catch (Exception e) {
            log.error("Failed constructing start nodes array", e);
            return;
        }

        if (forkJoinPool == null) forkJoinPool = new ForkJoinPool();
        threadWriters = new HashMap<>();

        if (exportMetadata) {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HHmm");
            Date date = new Date();
            String datedSubfolderName = format.format(date);
            this.datedExportFolder = Paths.get(exportDestination).resolve(datedSubfolderName);
        }

        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
        String startDateTime = sdf.format(cal.getTime());
        long startTime = System.currentTimeMillis();

        log.info(String.format("TreeDump starting - %s", startDateTime));
        log.info(String.format("    Check existence on disk: %s", checkPhysicalExistence));
        log.info(String.format("         Content store base: %s", contentStoreBase));
        log.info(String.format("              Get cm:name's: %s", exportNodeBuilder.getUseCmName()));
        log.info(String.format("       Root nodes read from: %s", rootNodeListInput));
        log.info(String.format("         Results written to: %s", csvOutputDir));
        log.info(String.format("         Metadata export is: %s", exportMetadata ? "ON" : "OFF"));
        if (exportMetadata)
            log.info(String.format("Metadata export destination: %s", datedExportFolder.toString()));

        dumpTreeTask = new TreeDumperTask(startNodes, 0);
        forkJoinPool.execute(dumpTreeTask);
        do {
            log.info("******************************************");
            log.info(String.format("Main: Parallelism: %d", forkJoinPool.getParallelism()));
            log.info(String.format("Main: Active Threads: %d", forkJoinPool.getActiveThreadCount()));
            log.info(String.format("Main: Task Count: %d", forkJoinPool.getQueuedTaskCount()));
            log.info(String.format("Main: Steal Count: %d", forkJoinPool.getStealCount()));
            log.info("******************************************");
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } while (!dumpTreeTask.isDone());

        for (BufferedWriter writer : threadWriters.values()) {
            try {
                writer.close();
            } catch (IOException e) {
                log.error("Failed to close BufferedWriter", e);
            }
        }
        threadWriters.clear();

        log.info("TreeDump finished");
        log.info(String.format("TreeDump elapsed time - %s seconds", (System.currentTimeMillis() - startTime) / 1000));
    }

    void shutdown() {
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
        }
        else {
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
                try {
                    processChildNodesDumpingToCsv(parentNode);
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

        private void processChildNodesDumpingToCsv(ExportNode parentNode) {
            List<ChildAssociationRef> childAssocs =
                    nodeService.getChildAssocs(parentNode.nodeRef, ContentModel.ASSOC_CONTAINS, RegexQNamePattern.MATCH_ALL);
            if (childAssocs.size() == 0) {
                return;
            }
            for (ChildAssociationRef assoc : childAssocs) {
                try {
                    ExportNode childNode = exportNodeBuilder.constructExportNode(assoc, parentNode.fullPath);
                    if (childNode.isFolder) {
                        subFolders.add(childNode);
                    }
                    if (checkExistenceForFilesAndOnlyIfRequested(childNode)) {
                        if (exportMetadata) {
                            exportMetadataToFileStructure(childNode);
                        }
                        writeCsvLine(
                                threadWriters.get(Thread.currentThread().getName()),
                                assoc.getParentRef().getId(),
                                assoc.getChildRef().getId(),
                                childNode.name,
                                childNode.nodeType,
                                recursionLevel,
                                childNode.isFolder,
                                parentNode.fullPath,
                                childNode.contentUrl,
                                childNode.contentBytes);
                    }
                } catch (Exception e) {
                    log.error("Failed to process child " + assoc.getQName().toPrefixString(namespaceService), e);
                }
            }
        }

    }

    private boolean checkExistenceForFilesAndOnlyIfRequested(ExportNode exportNode) {
        return !exportNode.isFile
                || (!checkPhysicalExistence || existsOnDisk(exportNode.nodeRef));
    }

    private boolean existsOnDisk(NodeRef childRef) {
        ContentData contentData = (ContentData) nodeService.getProperty(childRef,
                QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "content"));
        String contentURL = contentData.getContentUrl();
        String contentLocationWithinStore = contentURL.replace("store://", "");
        Path pathToCheck = Paths.get(contentStoreBase, contentLocationWithinStore);
        boolean fileExists = Files.exists(pathToCheck);
        if (!fileExists) log.warn(String.format("existence check FAILED for: %s", pathToCheck.toString()));
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
        createDirectory(node);
        try {
            wirelineExportService.writePropertyAndTranslationsFile(node, datedExportFolder);
            wirelineExportService.writeAclFile(node, datedExportFolder);
        } catch (Exception e) {
            log.error("Error by processing node " + node.fullPath);
            throw new RuntimeException(e);
        }
    }

    private void createDirectory(ExportNode node) {
        try {
            Path pathToCreate = node.isFolder ? Paths.get(node.fullPath) : Paths.get(node.fullPath).getParent();
            if (pathToCreate.getRoot() != null) {
                pathToCreate = pathToCreate.subpath(0, pathToCreate.getNameCount());
            }
            Files.createDirectories(datedExportFolder.resolve(pathToCreate));
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }

}

