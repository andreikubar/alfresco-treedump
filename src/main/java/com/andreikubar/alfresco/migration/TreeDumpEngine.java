package com.andreikubar.alfresco.migration;

import com.andreikubar.alfresco.migration.export.ExportNode;
import com.andreikubar.alfresco.migration.export.ExportNodeBuilder;
import org.alfresco.model.ContentModel;
import org.alfresco.repo.security.authentication.AuthenticationUtil;
import org.alfresco.service.ServiceRegistry;
import org.alfresco.service.cmr.dictionary.DictionaryService;
import org.alfresco.service.cmr.repository.*;
import org.alfresco.service.namespace.NamespaceService;
import org.alfresco.service.namespace.QName;
import org.alfresco.service.namespace.RegexQNamePattern;
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
    private String csvOutputDir;
    private String rootNodeListInput;
    private Boolean checkPhysicalExistence;
    private String contentStoreBase;

    private Log log = LogFactory.getLog(TreeDumpEngine.class);
    private ServiceRegistry serviceRegistry;
    private ExportNodeBuilder exportNodeBuilder;
    private NodeService nodeService;
    private NamespaceService namespaceService;
    private DictionaryService dictionaryService;
    private TreeDumperTask dumpTreeTask;
    private Map<String, BufferedWriter> threadWriters;

    private ForkJoinPool forkJoinPool;

    public TreeDumpEngine(ServiceRegistry serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
        nodeService = serviceRegistry.getNodeService();
        namespaceService = serviceRegistry.getNamespaceService();
        dictionaryService = serviceRegistry.getDictionaryService();
        if (checkPhysicalExistence == null) checkPhysicalExistence = true;
    }

    public void setCsvOutputDir(String csvOutputDir) {
        this.csvOutputDir = csvOutputDir;
    }

    public void setRootNodeListInput(String rootNodeListInput) {
        this.rootNodeListInput = rootNodeListInput;
    }

    public void setCheckPhysicalExistence(Boolean checkPhysicalExistence) {
        this.checkPhysicalExistence = checkPhysicalExistence;
    }

    public void setContentStoreBase(String contentStoreBase) {
        this.contentStoreBase = contentStoreBase;
    }

    public void setExportNodeBuilder(ExportNodeBuilder exportNodeBuilder) {
        this.exportNodeBuilder = exportNodeBuilder;
    }

    void startAndWait() {
        AuthenticationUtil.setAdminUserAsFullyAuthenticatedUser();
        List<ExportNode> startNodes = new ArrayList<>();
        try (BufferedReader bufferedReader =
                     new BufferedReader(new FileReader(rootNodeListInput))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                ExportNode startNode = new ExportNode();
                startNode.nodeRef = new NodeRef(StoreRef.STORE_REF_WORKSPACE_SPACESSTORE, line);
                startNode.name = (String)nodeService.getProperty(startNode.nodeRef, ContentModel.PROP_NAME);
                startNode.fullPath = "/" + startNode.name;
                startNodes.add(startNode);
            }
        } catch (IOException e) {
            log.error("Failed to read the root node list", e);
            return;
        }
        catch (Exception e){
            log.error("Failed constructing start nodes array", e);
            return;
        }

        Path csvOutPath = Paths.get(csvOutputDir);
        try {
            Files.createDirectories(csvOutPath);
        } catch (IOException e) {
            log.error("Failed to create output directory", e);
        }

        if (forkJoinPool == null) forkJoinPool = new ForkJoinPool();
        threadWriters = new HashMap<>();

        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
        String startDateTime = sdf.format(cal.getTime());
        long startTime = System.currentTimeMillis();

        /* TODO: remove the overridden values */
        checkPhysicalExistence = true;

        log.info(String.format("TreeDump starting - %s", startDateTime));
        log.info(String.format("Check existence on disk: %s", checkPhysicalExistence));
        log.info(String.format("     Content store base: %s", contentStoreBase));
        log.info(String.format("          Get cm:name's: %s", exportNodeBuilder.getUseCmName()));
        log.info(String.format("   Root nodes read from: %s", rootNodeListInput));
        log.info(String.format("     Results written to: %s", csvOutputDir));
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

        for (BufferedWriter writer : threadWriters.values()){
            try {
                writer.close();
            } catch (IOException e) {
                log.error("Failed to close BufferedWriter", e);
            }
        }
        threadWriters.clear();

        log.info("TreeDump finished");
        log.info(String.format("TreeDump elapsed time - %s seconds", (System.currentTimeMillis() - startTime)/1000 ));
    }

    void shutdown() {
        this.dumpTreeTask.cancel(true);
        this.forkJoinPool.shutdownNow();
        log.info("TreeDump shutdown");
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
            if (!threadWriters.containsKey(Thread.currentThread().getName())){
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
            try {
                for (ExportNode parentNode : parentNodes) {
                    processChildNodesDumpingToCsv(parentNode);
                    processSubFoldersRecursively();
                }
            } catch (Exception e) {
                log.error("Error by processing child-nodes", e);
            }
        }

        private void processSubFoldersRecursively() {
            if (subFolders.size() > 0) {
                if (subFolders.size() >= 2) {
                    TreeDumperTask otherTask = new TreeDumperTask(subFolders.subList(0,
                            (int) Math.floor(subFolders.size() / 2)), recursionLevel + 1);
                    otherTask.fork();
                    (new TreeDumperTask(subFolders.subList(
                            (int) Math.ceil(subFolders.size() / 2), subFolders.size()), recursionLevel + 1))
                            .compute();
                    otherTask.join();
                } else {
                    (new TreeDumperTask(subFolders, recursionLevel + 1)).compute();
                }
                subFolders.clear();
            }

        }

        private void processChildNodesDumpingToCsv(ExportNode parentNode) throws IOException {
            List<ChildAssociationRef> childAssocs =
                    nodeService.getChildAssocs(parentNode.nodeRef, ContentModel.ASSOC_CONTAINS, RegexQNamePattern.MATCH_ALL);
            if (childAssocs.size() > 0) {
                try {
                    for (ChildAssociationRef assoc : childAssocs) {
                        ExportNode childNode = exportNodeBuilder.constructExportNode(assoc, parentNode.fullPath);
                        if (childNode.isFolder) {
                            subFolders.add(childNode);
                        }
                        if (checkExistenceForFilesAndOnlyIfRequested(childNode)) {
                            writeCsvLine(
                                    threadWriters.get(Thread.currentThread().getName()),
                                    assoc.getParentRef().getId(),
                                    assoc.getChildRef().getId(),
                                    childNode.name, childNode.nodeType, recursionLevel,
                                    childNode.isFolder, parentNode.fullPath);
                        }
                    }
                } catch (FileNotFoundException e) {
                    log.error("Failed to write output", e);
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
            if (!fileExists) log.debug(String.format("existence check %s for: %s", "FAILED", pathToCheck.toString()));
            return fileExists;
        }

        private void writeCsvLine(BufferedWriter csvWriter, String parentId, String childId, String nodeName,
                                  QName childNodeType, int level, boolean isFolder, String containingPath) throws IOException {
            String csvLine = String.format("%d|%s|%s|%s|%s|%s|%s\n",
                    level,
                    parentId,
                    childId,
                    childNodeType.getPrefixedQName(namespaceService).getPrefixString(),
                    isFolder?"isFolder":"notAFolder",
                    containingPath,
                    nodeName
                    );
            csvWriter.append(csvLine);
        }

    }
}
