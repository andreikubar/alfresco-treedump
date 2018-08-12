package com.andreikubar.alfresco.migration.importer;


import org.alfresco.service.ServiceRegistry;

public class ImportEngine {
    private ServiceRegistry serviceRegistry;

    public ImportEngine(ServiceRegistry serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
    }

    // here we make a REST call to the export endpoint

}
