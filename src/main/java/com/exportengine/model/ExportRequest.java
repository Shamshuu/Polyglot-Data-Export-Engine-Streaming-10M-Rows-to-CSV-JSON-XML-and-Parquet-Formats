package com.exportengine.model;

import java.util.List;

public class ExportRequest {
    private String format;
    private List<ColumnMapping> columns;
    private String compression;

    public ExportRequest() {}

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
}
