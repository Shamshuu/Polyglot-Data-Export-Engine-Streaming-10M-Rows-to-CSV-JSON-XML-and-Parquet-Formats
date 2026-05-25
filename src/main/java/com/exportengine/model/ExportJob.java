package com.exportengine.model;

import java.util.List;
import java.util.UUID;

public class ExportJob {
    private UUID exportId;
    private String format;
    private List<ColumnMapping> columns;
    private String compression;
    private String status;

    public ExportJob() {}

    public ExportJob(UUID exportId, String format, List<ColumnMapping> columns, String compression, String status) {
        this.exportId = exportId;
        this.format = format;
        this.columns = columns;
        this.compression = compression;
        this.status = status;
    }

    public UUID getExportId() {
        return exportId;
    }

    public void setExportId(UUID exportId) {
        this.exportId = exportId;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public List<ColumnMapping> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnMapping> columns) {
        this.columns = columns;
    }

    public String getCompression() {
        return compression;
    }

    public void setCompression(String compression) {
        this.compression = compression;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
