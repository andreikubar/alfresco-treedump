package com.swisscom;

import org.alfresco.model.ContentModel;
import org.alfresco.repo.security.authentication.AuthenticationUtil;
import org.alfresco.service.ServiceRegistry;
import org.alfresco.service.cmr.repository.ChildAssociationRef;
import org.alfresco.service.cmr.repository.NodeRef;
import org.alfresco.service.cmr.repository.NodeService;
import org.alfresco.service.cmr.repository.StoreRef;
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
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;

public class TreeDumpEngine {
    private String csvOutputDir;
    private String rootNodeListInput;
    private static final QName FOLDER_TYPE = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "folder");
    private static final QName FILE_TYPE = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "content");
    private Log log = LogFactory.getLog(TreeDumpEngine.class);
    private ServiceRegistry serviceRegistry;
    private NodeService nodeService;
    private NamespaceService namespaceService;
    private TreeDumperTask dumpTreeTask;

    private ForkJoinPool forkJoinPool;

    public TreeDumpEngine(ServiceRegistry serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
    }

    public void setCsvOutputDir(String csvOutputDir) {
        this.csvOutputDir = csvOutputDir;
    }

    public void setRootNodeListInput(String rootNodeListInput) {
        this.rootNodeListInput = rootNodeListInput;
    }

    void startAndWait(){
        nodeService = serviceRegistry.getNodeService();
        namespaceService = serviceRegistry.getNamespaceService();

        List<NodeRef> startNodes = new ArrayList<>();
        try(BufferedReader bufferedReader =
                    new BufferedReader(new FileReader(rootNodeListInput))){
            String line;
            while((line = bufferedReader.readLine()) != null){
                startNodes.add(new NodeRef(StoreRef.STORE_REF_WORKSPACE_SPACESSTORE, line));
            }
        }
        catch (IOException e){
            log.error(e);
            return;
        }

        Path csvOutDir = Paths.get(csvOutputDir);
        try {
            Files.createDirectories(csvOutDir);
        } catch (IOException e) {
            log.error(e);
        }

        forkJoinPool = new ForkJoinPool();
        log.info("TreeDump starting");
        dumpTreeTask = new TreeDumperTask(startNodes, 0);
        forkJoinPool.execute(dumpTreeTask);
        do {
            log.info("******************************************\n");
            log.info(String.format("Main: Parallelism: %d\n", forkJoinPool.getParallelism()));
            log.info(String.format("Main: Active Threads: %d\n", forkJoinPool.getActiveThreadCount()));
            log.info(String.format("Main: Task Count: %d\n", forkJoinPool.getQueuedTaskCount()));
            log.info(String.format("Main: Steal Count: %d\n", forkJoinPool.getStealCount()));
            log.info("******************************************\n");
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } while (!dumpTreeTask.isDone());

        log.info("TreeDump finished");
    }

    void shutdown(){
        this.dumpTreeTask.cancel(true);
        this.forkJoinPool.shutdownNow();
        log.info("TreeDump shutdown");
    }


    private class TreeDumperTask extends RecursiveAction {
        private List<NodeRef> startNodes;
        private List<NodeRef> subFolderNodes = new ArrayList<>(50);
        private int recursionLevel;
        private Path csvOutFile = Paths.get(csvOutputDir,
                String.format("%s-%s.csv", "treedump", Thread.currentThread().getName()));

        TreeDumperTask(List<NodeRef> startNodes, int recursionLevel) {
            this.startNodes = startNodes;
            this.recursionLevel = recursionLevel;
        }

        @Override
        public void compute() {
            AuthenticationUtil.setAdminUserAsFullyAuthenticatedUser();
            try {
                for (NodeRef parent : startNodes){
                    dumpChildrenToCsv(parent);
                    processSubFoldersRecursively(parent);
                }
            } catch (Exception e){
                log.error(e);
            }
        }

        private void processSubFoldersRecursively(NodeRef parent) {
            if (subFolderNodes.size() > 0) {
                if (subFolderNodes.size() >= 2) {
                    TreeDumperTask otherTask = new TreeDumperTask(subFolderNodes.subList(0,
                            (int) Math.floor(subFolderNodes.size() / 2)), recursionLevel + 1);
                    otherTask.fork();
                    (new TreeDumperTask(subFolderNodes.subList(
                            (int) Math.ceil(subFolderNodes.size() / 2), subFolderNodes.size()), recursionLevel + 1))
                            .compute();
                    otherTask.join();
                } else {
                    (new TreeDumperTask(subFolderNodes, recursionLevel + 1)).compute();
                }
                subFolderNodes.clear();
            }

        }

        private void dumpChildrenToCsv(NodeRef parent) throws IOException {
            List<ChildAssociationRef> childAssocs =
                    nodeService.getChildAssocs(parent, ContentModel.ASSOC_CONTAINS, RegexQNamePattern.MATCH_ALL );
            if (childAssocs.size() > 0) {
                try(BufferedWriter csvWriter = new BufferedWriter(
                        new OutputStreamWriter(
                                new FileOutputStream(csvOutFile.toFile(),true), StandardCharsets.UTF_8))) {
                    for (ChildAssociationRef assoc : childAssocs) {
                        QName childNodeType = nodeService.getType(assoc.getChildRef());
                        if (childNodeType.equals(FOLDER_TYPE)){
                            subFolderNodes.add(assoc.getChildRef());
                        }
                        writeCsvLIne(csvWriter, assoc.getParentRef().getId(), assoc.getChildRef().getId(),
                                assoc.getQName().getLocalName(), childNodeType, recursionLevel);
                    }
                }
                catch (FileNotFoundException e) {
                    log.error(e);
                }
            }
        }

        private void dumpNodeRecursively(NodeRef rootNode, NodeService nodeService, BufferedWriter csvWriter, int level)
                throws IOException {
            List<ChildAssociationRef> childAssocs = nodeService.getChildAssocs(rootNode, ContentModel.ASSOC_CONTAINS,
                    RegexQNamePattern.MATCH_ALL);
            level++;
            for (ChildAssociationRef assoc : childAssocs) {
                QName childNodeType = nodeService.getType(assoc.getChildRef());
                writeCsvLIne(csvWriter, assoc.getParentRef().getId(), assoc.getChildRef().getId(),
                        assoc.getQName().getLocalName(), childNodeType, level);
                if (childNodeType.equals(FOLDER_TYPE)){
                    dumpNodeRecursively(assoc.getChildRef(), nodeService, csvWriter, level);
                }
            }
        }

        private void writeCsvLIne(BufferedWriter csvWriter, String parentId, String childId, String nodeLocalName,
                                  QName childNodeType, int level) throws IOException {
            String csvLine = String.format("%d,%s,%s,%s,%s\n",
                    level,
                    parentId,
                    childId,
                    nodeLocalName,
                    childNodeType.getPrefixedQName(namespaceService).getPrefixString()
            );
            csvWriter.append(csvLine);
        }

    }
}
