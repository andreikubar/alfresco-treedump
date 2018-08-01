package com.mimacom.alfresco.migration;

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
    private Boolean useCmName;
    private static final QName FOLDER_TYPE = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "folder");
    private static final QName FILE_TYPE = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "content");
    private Log log = LogFactory.getLog(TreeDumpEngine.class);
    private ServiceRegistry serviceRegistry;
    private NodeService nodeService;
    private NamespaceService namespaceService;
    private DictionaryService dictionaryService;
    private TreeDumperTask dumpTreeTask;

    private ForkJoinPool forkJoinPool;

    public TreeDumpEngine(ServiceRegistry serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
        nodeService = serviceRegistry.getNodeService();
        namespaceService = serviceRegistry.getNamespaceService();
        dictionaryService = serviceRegistry.getDictionaryService();
        if (checkPhysicalExistence == null) checkPhysicalExistence = true;
        if (useCmName == null) useCmName = true;
    }

    public void setCsvOutputDir(String csvOutputDir) {
        this.csvOutputDir = csvOutputDir;
    }

    public void setRootNodeListInput(String rootNodeListInput) {
        this.rootNodeListInput = rootNodeListInput;
    }

    public void setUseCmName(Boolean useCmName) {
        this.useCmName = useCmName;
    }

    public void setCheckPhysicalExistence(Boolean checkPhysicalExistence) {
        this.checkPhysicalExistence = checkPhysicalExistence;
    }

    public void setContentStoreBase(String contentStoreBase) {
        this.contentStoreBase = contentStoreBase;
    }

    void startAndWait() {
        AuthenticationUtil.setAdminUserAsFullyAuthenticatedUser();
        List<ExportNode> startNodes = new ArrayList<>();
        try (BufferedReader bufferedReader =
                     new BufferedReader(new FileReader(rootNodeListInput))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                NodeRef startParentNode = new NodeRef(StoreRef.STORE_REF_WORKSPACE_SPACESSTORE, line);
                String startParentNodeName  = (String)nodeService.getProperty(startParentNode, ContentModel.PROP_NAME);
                startNodes.add(new ExportNode(new NodeRef(StoreRef.STORE_REF_WORKSPACE_SPACESSTORE, line), "/" + startParentNodeName));
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

        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
        String startDateTime = sdf.format(cal.getTime());
        long startTime = System.currentTimeMillis();

        checkPhysicalExistence = true;
        useCmName = true;

        log.info(String.format("TreeDump starting - %s", startDateTime));
        log.info(String.format("Check existence on disk: %s", checkPhysicalExistence));
        log.info(String.format("     Content store base: %s", contentStoreBase));
        log.info(String.format("          Get cm:name's: %s", useCmName));
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
        private Path csvOutFile = Paths.get(csvOutputDir,
                String.format("%s-%s.csv", "treedump", Thread.currentThread().getName()));

        TreeDumperTask(List<ExportNode> parentNodes, int recursionLevel) {
            this.parentNodes = parentNodes;
            this.recursionLevel = recursionLevel;
        }

        @Override
        public void compute() {
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
                try (BufferedWriter csvWriter = new BufferedWriter(
                        new OutputStreamWriter(
                                new FileOutputStream(csvOutFile.toFile(), true), StandardCharsets.UTF_8))) {
                    for (ChildAssociationRef assoc : childAssocs) {
                        QName childNodeType = nodeService.getType(assoc.getChildRef());
                        String childNodeName = getChildNodeName(assoc);
                        boolean isFolder = dictionaryService.isSubClass(childNodeType, FOLDER_TYPE);
                        if (isFolder) {
                            subFolders.add(new ExportNode(assoc.getChildRef(), parentNode.fullPath + "/" + childNodeName));
                        }
                        if (checkExistenceForFilesAndOnlyIfRequested(assoc, childNodeType)) {
                            writeCsvLIne(csvWriter, assoc.getParentRef().getId(), assoc.getChildRef().getId(),
                                    childNodeName, childNodeType, recursionLevel, isFolder, parentNode.fullPath);
                        }
                    }
                } catch (FileNotFoundException e) {
                    log.error("Failed to write output", e);
                }
            }
        }

        private boolean checkExistenceForFilesAndOnlyIfRequested(ChildAssociationRef assoc, QName childNodeType) {
            return !dictionaryService.isSubClass(childNodeType, FILE_TYPE)
                    || (!checkPhysicalExistence || existsOnDisk(assoc.getChildRef()));
        }

        private String getChildNodeName(ChildAssociationRef assoc) {
            String childNodeName;
            if (useCmName) {
                childNodeName = (String) nodeService.getProperty(assoc.getChildRef(), ContentModel.PROP_NAME);
            }
            else {
                childNodeName = assoc.getQName().getLocalName();
            }
            return childNodeName;
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

        private void dumpNodeRecursively(NodeRef rootNode, NodeService nodeService, BufferedWriter csvWriter, int level)
                throws IOException {
            List<ChildAssociationRef> childAssocs = nodeService.getChildAssocs(rootNode, ContentModel.ASSOC_CONTAINS,
                    RegexQNamePattern.MATCH_ALL);
            level++;
            for (ChildAssociationRef assoc : childAssocs) {
                QName childNodeType = nodeService.getType(assoc.getChildRef());
                writeCsvLIne(csvWriter, assoc.getParentRef().getId(), assoc.getChildRef().getId(),
                        assoc.getQName().getLocalName(), childNodeType, level, childNodeType.equals(FOLDER_TYPE),"");
                if (childNodeType.equals(FOLDER_TYPE)) {
                    dumpNodeRecursively(assoc.getChildRef(), nodeService, csvWriter, level);
                }
            }
        }

        private void writeCsvLIne(BufferedWriter csvWriter, String parentId, String childId, String nodeLocalName,
                                  QName childNodeType, int level, boolean isFolder, String fullPath) throws IOException {
            String csvLine = String.format("%d,%s,%s,%s,%s,%s,%s\n",
                    level,
                    parentId,
                    childId,
                    nodeLocalName,
                    childNodeType.getPrefixedQName(namespaceService).getPrefixString(),
                    isFolder?"isFolder":"notAFolder",
                    fullPath
            );
            csvWriter.append(csvLine);
        }

    }

    private class ExportNode {
        private NodeRef nodeRef;
        private String fullPath;

        ExportNode(NodeRef nodeRef, String fullPath) {
            this.nodeRef = nodeRef;
            this.fullPath = fullPath;
        }
    }
}
