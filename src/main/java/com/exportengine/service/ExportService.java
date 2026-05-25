package com.exportengine.service;

import com.exportengine.model.ColumnMapping;
import com.exportengine.model.ExportJob;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPOutputStream;

@Service
public class ExportService {

    private static final Set<String> ALLOWED_COLUMNS = Set.of("id", "created_at", "name", "value", "metadata");
    private final ConcurrentHashMap<UUID, ExportJob> jobs = new ConcurrentHashMap<>();
    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private DataSource dataSource;

    public ExportJob createJob(String format, List<ColumnMapping> columns, String compression) {
        // Validate inputs
        if (format == null || !Set.of("csv", "json", "xml", "parquet").contains(format.toLowerCase())) {
            throw new IllegalArgumentException("Invalid format: " + format);
        }
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Columns selection mapping is required");
        }
        for (ColumnMapping col : columns) {
            if (!ALLOWED_COLUMNS.contains(col.getSource().toLowerCase())) {
                throw new IllegalArgumentException("Invalid/unauthorized source column: " + col.getSource());
            }
            if (col.getTarget() == null || !col.getTarget().matches("^[a-zA-Z_][a-zA-Z0-9_]*$")) {
                throw new IllegalArgumentException("Invalid target column name: " + col.getTarget());
            }
        }
        if (compression != null && !"gzip".equalsIgnoreCase(compression)) {
            throw new IllegalArgumentException("Unsupported compression: " + compression);
        }
        if ("gzip".equalsIgnoreCase(compression) && "parquet".equalsIgnoreCase(format)) {
            throw new IllegalArgumentException("Gzip compression cannot be applied to Parquet at the transport layer.");
        }

        UUID jobId = UUID.randomUUID();
        ExportJob job = new ExportJob(jobId, format.toLowerCase(), columns, compression, "pending");
        jobs.put(jobId, job);
        return job;
    }

    public ExportJob getJob(UUID jobId) {
        return jobs.get(jobId);
    }

    public void streamExport(ExportJob job, OutputStream originalOut) throws Exception {
        boolean isGzip = "gzip".equalsIgnoreCase(job.getCompression()) && !"parquet".equals(job.getFormat());
        
        try (OutputStream out = isGzip ? new GZIPOutputStream(originalOut) : originalOut) {
            if ("parquet".equals(job.getFormat())) {
                // Parquet must be written to a seekable file on disk first
                File tempFile = File.createTempFile("export_" + job.getExportId(), ".parquet");
                try {
                    writeParquetToDisk(job, tempFile);
                    // Now stream the file to the client in small buffers
                    try (InputStream fis = new BufferedInputStream(new FileInputStream(tempFile))) {
                        byte[] buffer = new byte[65536]; // 64KB
                        int bytesRead;
                        while ((bytesRead = fis.read(buffer)) != -1) {
                            out.write(buffer, 0, bytesRead);
                        }
                    }
                } finally {
                    if (tempFile.exists()) {
                        tempFile.delete();
                    }
                }
            } else {
                // Stream on-the-fly from the database
                String query = buildQuery(job.getColumns());
                
                try (Connection conn = dataSource.getConnection()) {
                    conn.setAutoCommit(false); // Crucial: PostgreSQL cursor streaming requires autoCommit=false
                    
                    try (PreparedStatement stmt = conn.prepareStatement(query)) {
                        stmt.setFetchSize(10000); // Fetch 10,000 rows at a time
                        
                        try (ResultSet rs = stmt.executeQuery()) {
                            switch (job.getFormat()) {
                                case "csv":
                                    writeCsv(rs, job.getColumns(), out);
                                    break;
                                case "json":
                                    writeJson(rs, job.getColumns(), out);
                                    break;
                                case "xml":
                                    writeXml(rs, job.getColumns(), out);
                                    break;
                            }
                        }
                    }
                }
            }
            
            if (out instanceof GZIPOutputStream) {
                ((GZIPOutputStream) out).finish();
            }
        }
    }

    private String buildQuery(List<ColumnMapping> columns) {
        StringBuilder sb = new StringBuilder("SELECT ");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(columns.get(i).getSource()); // Safe: source is validated against whitelist
        }
        sb.append(" FROM records ORDER BY id ASC");
        return sb.toString();
    }

    private void writeCsv(ResultSet rs, List<ColumnMapping> columns, OutputStream out) throws Exception {
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
        
        // Header
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) writer.write(",");
            writer.write(escapeCsv(columns.get(i).getTarget()));
        }
        writer.write("\n");

        // Data Rows
        while (rs.next()) {
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) writer.write(",");
                String sourceCol = columns.get(i).getSource();
                String valStr;
                if ("metadata".equals(sourceCol)) {
                    valStr = rs.getString(sourceCol);
                } else {
                    Object val = rs.getObject(sourceCol);
                    valStr = (val != null) ? val.toString() : null;
                }
                writer.write(escapeCsv(valStr));
            }
            writer.write("\n");
        }
        writer.flush();
    }

    private String escapeCsv(String val) {
        if (val == null) return "";
        if (val.contains(",") || val.contains("\"") || val.contains("\n") || val.contains("\r")) {
            return "\"" + val.replace("\"", "\"\"") + "\"";
        }
        return val;
    }

    private void writeJson(ResultSet rs, List<ColumnMapping> columns, OutputStream out) throws Exception {
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
        writer.write("[");
        
        boolean first = true;
        while (rs.next()) {
            if (!first) {
                writer.write(",");
            } else {
                first = false;
            }
            writer.write("{");
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) writer.write(",");
                String sourceCol = columns.get(i).getSource();
                String targetCol = columns.get(i).getTarget();
                writer.write("\"" + targetCol + "\":");
                
                Object val = rs.getObject(sourceCol);
                if (val == null) {
                    writer.write("null");
                } else if ("metadata".equals(sourceCol)) {
                    // Embed raw JSONB string directly
                    writer.write(rs.getString(sourceCol));
                } else if (val instanceof Number || val instanceof Boolean) {
                    writer.write(val.toString());
                } else {
                    writer.write("\"" + escapeJson(val.toString()) + "\"");
                }
            }
            writer.write("}");
        }
        writer.write("]");
        writer.flush();
    }

    private String escapeJson(String val) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < val.length(); i++) {
            char ch = val.charAt(i);
            switch (ch) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\b': sb.append("\\b"); break;
                case '\f': sb.append("\\f"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (ch < ' ') {
                        String hex = Integer.toHexString(ch);
                        sb.append("\\u");
                        for (int k = 0; k < 4 - hex.length(); k++) {
                            sb.append('0');
                        }
                        sb.append(hex.toLowerCase());
                    } else {
                        sb.append(ch);
                    }
            }
        }
        return sb.toString();
    }

    private void writeXml(ResultSet rs, List<ColumnMapping> columns, OutputStream out) throws Exception {
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
        writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<records>\n");
        
        while (rs.next()) {
            writer.write("  <record>\n");
            for (ColumnMapping col : columns) {
                String sourceCol = col.getSource();
                String targetCol = col.getTarget();
                
                if ("metadata".equals(sourceCol)) {
                    String jsonStr = rs.getString(sourceCol);
                    if (jsonStr == null) {
                        writer.write("    <" + targetCol + "/>\n");
                    } else {
                        JsonNode node = mapper.readTree(jsonStr);
                        writer.write("    ");
                        writeJsonNodeAsXml(node, targetCol, writer);
                        writer.write("\n");
                    }
                } else {
                    Object val = rs.getObject(sourceCol);
                    if (val == null) {
                        writer.write("    <" + targetCol + "/>\n");
                    } else {
                        writer.write("    <" + targetCol + ">" + escapeXml(val.toString()) + "</" + targetCol + ">\n");
                    }
                }
            }
            writer.write("  </record>\n");
        }
        writer.write("</records>\n");
        writer.flush();
    }

    private void writeJsonNodeAsXml(JsonNode node, String tagName, BufferedWriter writer) throws IOException {
        if (node.isNull() || node.isMissingNode()) {
            writer.write("<" + tagName + "/>");
        } else if (node.isObject()) {
            writer.write("<" + tagName + ">");
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                writeJsonNodeAsXml(field.getValue(), field.getKey(), writer);
            }
            writer.write("</" + tagName + ">");
        } else if (node.isArray()) {
            writer.write("<" + tagName + ">");
            for (JsonNode child : node) {
                writeJsonNodeAsXml(child, "item", writer);
            }
            writer.write("</" + tagName + ">");
        } else {
            writer.write("<" + tagName + ">" + escapeXml(node.asText()) + "</" + tagName + ">");
        }
    }

    private String escapeXml(String val) {
        if (val == null) return "";
        return val.replace("&", "&amp;")
                  .replace("<", "&lt;")
                  .replace(">", "&gt;")
                  .replace("\"", "&quot;")
                  .replace("'", "&apos;");
    }

    private void writeParquetToDisk(ExportJob job, File tempFile) throws Exception {
        // Build dynamic schema
        StringBuilder schemaBuilder = new StringBuilder("message Export {\n");
        for (ColumnMapping col : job.getColumns()) {
            String source = col.getSource();
            String target = col.getTarget();
            if ("id".equals(source)) {
                schemaBuilder.append("  optional int64 ").append(target).append(";\n");
            } else if ("created_at".equals(source)) {
                schemaBuilder.append("  optional binary ").append(target).append(" (UTF8);\n");
            } else if ("name".equals(source)) {
                schemaBuilder.append("  optional binary ").append(target).append(" (UTF8);\n");
            } else if ("value".equals(source)) {
                schemaBuilder.append("  optional double ").append(target).append(";\n");
            } else if ("metadata".equals(source)) {
                schemaBuilder.append("  optional group ").append(target).append(" {\n")
                        .append("    optional binary sku (UTF8);\n")
                        .append("    optional group details {\n")
                        .append("      optional double weight;\n")
                        .append("      optional group dimensions {\n")
                        .append("        optional double width;\n")
                        .append("        optional double height;\n")
                        .append("      }\n")
                        .append("    }\n")
                        .append("  }\n");
            }
        }
        schemaBuilder.append("}");
        
        MessageType schema = MessageTypeParser.parseMessageType(schemaBuilder.toString());
        Path path = new Path(tempFile.getAbsolutePath());
        Configuration conf = new Configuration();
        
        // Disable Hadoop file system caching and set simple configuration
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            String query = buildQuery(job.getColumns());
            
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setFetchSize(10000); // Stream in 10k rows batch
                
                try (ResultSet rs = stmt.executeQuery()) {
                    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
                            .withType(schema)
                            .withConf(conf)
                            .withRowGroupSize(1 * 1024 * 1024) // 1MB row group size keeps memory low
                            .withPageSize(64 * 1024)           // 64KB page size
                            .withCompressionCodec(CompressionCodecName.SNAPPY)
                            .build()) {
                        
                        while (rs.next()) {
                            SimpleGroup group = new SimpleGroup(schema);
                            for (ColumnMapping col : job.getColumns()) {
                                String source = col.getSource();
                                String target = col.getTarget();
                                Object val = rs.getObject(source);
                                if (val == null) {
                                    continue;
                                }
                                
                                if ("id".equals(source)) {
                                    group.add(target, rs.getLong(source));
                                } else if ("created_at".equals(source)) {
                                    group.add(target, rs.getString(source));
                                } else if ("name".equals(source)) {
                                    group.add(target, rs.getString(source));
                                } else if ("value".equals(source)) {
                                    group.add(target, rs.getDouble(source));
                                } else if ("metadata".equals(source)) {
                                    String jsonStr = rs.getString(source);
                                    JsonNode node = mapper.readTree(jsonStr);
                                    Group metaGroup = group.addGroup(target);
                                    
                                    if (node.has("sku") && !node.get("sku").isNull()) {
                                        metaGroup.add("sku", node.get("sku").asText());
                                    }
                                    if (node.has("details") && node.get("details").isObject()) {
                                        JsonNode details = node.get("details");
                                        Group detailsGroup = metaGroup.addGroup("details");
                                        if (details.has("weight") && !details.get("weight").isNull()) {
                                            detailsGroup.add("weight", details.get("weight").asDouble());
                                        }
                                        if (details.has("dimensions") && details.get("dimensions").isObject()) {
                                            JsonNode dimensions = details.get("dimensions");
                                            Group dimGroup = detailsGroup.addGroup("dimensions");
                                            if (dimensions.has("width") && !dimensions.get("width").isNull()) {
                                                dimGroup.add("width", dimensions.get("width").asDouble());
                                            }
                                            if (dimensions.has("height") && !dimensions.get("height").isNull()) {
                                                dimGroup.add("height", dimensions.get("height").asDouble());
                                            }
                                        }
                                    }
                                }
                            }
                            writer.write(group);
                        }
                    }
                }
            }
        }
    }

    // Benchmark Execution Method
    public Map<String, Object> executeBenchmark() throws Exception {
        // Query to check rows
        long count = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT COUNT(*) FROM records");
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                count = rs.getLong(1);
            }
        }

        List<Map<String, Object>> results = new ArrayList<>();
        List<String> formats = List.of("csv", "json", "xml", "parquet");
        List<ColumnMapping> columns = List.of(
            new ColumnMapping("id", "id"),
            new ColumnMapping("created_at", "created_at"),
            new ColumnMapping("name", "name"),
            new ColumnMapping("value", "value"),
            new ColumnMapping("metadata", "metadata")
        );

        for (String format : formats) {
            // Suggest GC before test
            System.gc();
            Thread.sleep(100);
            resetPeakMemory();

            UUID dummyId = UUID.randomUUID();
            ExportJob job = new ExportJob(dummyId, format, columns, null, "running");
            
            File tempFile = File.createTempFile("benchmark_" + format, "." + format);
            tempFile.deleteOnExit();

            long startTime = System.currentTimeMillis();
            
            // Perform export to file
            try (OutputStream fos = new BufferedOutputStream(new FileOutputStream(tempFile))) {
                streamExport(job, fos);
            }
            
            long endTime = System.currentTimeMillis();
            double durationSeconds = (endTime - startTime) / 1000.0;
            long fileSize = tempFile.length();
            double peakMemory = getPeakMemoryMB();
            
            tempFile.delete(); // Delete after measurement

            Map<String, Object> formatResult = new LinkedHashMap<>();
            formatResult.put("format", format);
            formatResult.put("durationSeconds", durationSeconds);
            formatResult.put("fileSizeBytes", fileSize);
            formatResult.put("peakMemoryMB", peakMemory);
            results.add(formatResult);
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("datasetRowCount", count);
        response.put("results", results);
        return response;
    }

    private void resetPeakMemory() {
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            pool.resetPeakUsage();
        }
    }

    private double getPeakMemoryMB() {
        long peakBytes = 0;
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            peakBytes += pool.getPeakUsage().getUsed();
        }
        return peakBytes / (1024.0 * 1024.0);
    }
}
