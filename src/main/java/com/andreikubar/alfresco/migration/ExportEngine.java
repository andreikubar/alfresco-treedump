package com.andreikubar.alfresco.migration;

import com.andreikubar.alfresco.migration.export.ExportNode;
import com.andreikubar.alfresco.migration.export.ExportNodeBuilder;
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
import java.util.*;
import java.util.concurrent.*;

public class ExportEngine {
    public static final int N_THREADS = 4;
    public static final int BATCH_SIZE = 50;
    public static CsvOutput csvOutput;
    public static Map<Integer, Map<String, BufferedWriter>> levelThreadWriters = new HashMap<>();
    private Log log = LogFactory.getLog(ExportEngine.class);

    private String rootNodeListInput;
    private String propertyFile;
    private String exportDestination;
    private String csvOutputDir;

    private NodeService nodeService;
    private NamespaceService namespaceService;
    private ContentStore defaultContentStore;
    private WirelineExportService wirelineExportService;
    private ExportNodeBuilder exportNodeBuilder;

    private ExecutorService executorService;
    CompletionService<Integer> completionService;

    public ExportEngine(ServiceRegistry serviceRegistry, ContentStore defaultContentStore) {
        this.defaultContentStore = defaultContentStore;
        nodeService = serviceRegistry.getNodeService();
        namespaceService = serviceRegistry.getNamespaceService();
    }

    public void setExportNodeBuilder(ExportNodeBuilder exportNodeBuilder) {
        this.exportNodeBuilder = exportNodeBuilder;
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
        } catch (FileNotFoundException e) {
            log.error("Property file not found " + this.propertyFile);
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void startLevelWiseExport(){
        AuthenticationUtil.setAdminUserAsFullyAuthenticatedUser();
        loadPropeties();
        csvOutput = new CsvOutput(csvOutputDir);
        levelThreadWriters = new HashMap<>();
        if (executorService == null || executorService.isShutdown()){
            executorService = Executors.newFixedThreadPool(N_THREADS);
        }
        completionService = new ExecutorCompletionService<>(executorService);

        List<ExportNode> startNodes = getStartNodes();
        List<List<ExportNode>> levels = new ArrayList<>();
        levels.add(startNodes);

        long startTime = System.currentTimeMillis();
        List<ExportNode> nextLevelNodes;
        for (int level = 0;; level++){
            if (levels.get(level).isEmpty()){
                log.debug("No more levels to process, last level was: " + (level-1));
                break;
            }
            log.info("Starting level " + level + " export");
            nextLevelNodes = new ArrayList<>();
            levels.add(nextLevelNodes);
            int nodeCounter = 0;
            int lastDispatched = 0;
            List<Future> exportTasks = new ArrayList<>();
            for (ExportNode parentNode : levels.get(level)) {
                List<ChildAssociationRef> childAssocs = nodeService.getChildAssocs(parentNode.nodeRef,
                        ContentModel.ASSOC_CONTAINS, RegexQNamePattern.MATCH_ALL);
                if (childAssocs.size() > 0) {
                    for (ChildAssociationRef childAssoc : childAssocs) {
                        nextLevelNodes.add(exportNodeBuilder.constructExportNode(childAssoc, parentNode.fullPath));
                        nodeCounter++;
                        if (nodeCounter - lastDispatched >= BATCH_SIZE){
                            exportTasks.add(
                                    dispatchCurrentBatch(
                                            new ArrayList<>(nextLevelNodes.subList(lastDispatched, nodeCounter)), level));
                            log.debug("Tasks submitted: " + exportTasks.size());
                            lastDispatched = nodeCounter;
                        }
                    }
                }
            }
            if (nodeCounter > lastDispatched){
                exportTasks.add(
                        dispatchCurrentBatch(nextLevelNodes.subList(lastDispatched, nodeCounter), level));
                log.debug("Tasks submitted: " + exportTasks.size());
            }
            if (exportTasks.size() > 0){
                for (int i = 1; i <= exportTasks.size(); i++){
                    try {
                        completionService.take().get();
                        log.debug("Tasks completed: " + i + " out of " + exportTasks.size());
                    } catch (ExecutionException| InterruptedException e) {
                        log.error("Export processing was interrupted");
                        throw new RuntimeException(e);
                    }
                }
                for (Map.Entry<String, BufferedWriter> threadWriter : levelThreadWriters.get(level).entrySet()){
                    try {
                        threadWriter.getValue().close();
                    } catch (IOException e) {
                        log.error("Failed to close threadWriter " + threadWriter.getKey() + " level " + level);
                    }
                }
                log.info("Level " + level + " export is finished");
            }

        }
        log.info(String.format("Level-wise export finished, elapsed time %s seconds", (System.currentTimeMillis() - startTime) / 1000));
    }

    private Future<?> dispatchCurrentBatch(List<ExportNode> exportNodes, int level) {
        ExportTask exportTask = new ExportTask(exportNodes, level);
        return completionService.submit(exportTask, 1);
    }


    private List<ExportNode> getStartNodes(){
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
        } catch (Exception e) {
            log.error("Failed constructing start nodes array", e);
        }
        return startNodes;
    }
}
