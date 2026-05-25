package com.exportengine.controller;

import com.exportengine.model.ExportJob;
import com.exportengine.model.ExportRequest;
import com.exportengine.service.ExportService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@RestController
public class ExportController {

    @Autowired
    private ExportService exportService;

    @PostMapping("/exports")
    public ResponseEntity<?> createExportJob(@RequestBody ExportRequest request) {
        try {
            ExportJob job = exportService.createJob(
                    request.getFormat(),
                    request.getColumns(),
                    request.getCompression()
            );

            Map<String, Object> response = new LinkedHashMap<>();
            response.put("exportId", job.getExportId().toString());
            response.put("status", job.getStatus());

            return ResponseEntity.status(HttpStatus.CREATED).body(response);
        } catch (IllegalArgumentException e) {
            Map<String, String> error = new LinkedHashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    @GetMapping("/exports/{exportId}/download")
    public ResponseEntity<StreamingResponseBody> downloadExport(@PathVariable UUID exportId) {
        ExportJob job = exportService.getJob(exportId);
        if (job == null) {
            return ResponseEntity.notFound().build();
        }

        HttpHeaders headers = new HttpHeaders();
        String format = job.getFormat().toLowerCase();
        boolean isGzip = "gzip".equalsIgnoreCase(job.getCompression());

        switch (format) {
            case "csv":
                headers.setContentType(MediaType.parseMediaType("text/csv"));
                headers.setContentDisposition(ContentDisposition.attachment()
                        .filename("export.csv" + (isGzip ? ".gz" : ""))
                        .build());
                break;
            case "json":
                headers.setContentType(MediaType.APPLICATION_JSON);
                headers.setContentDisposition(ContentDisposition.attachment()
                        .filename("export.json" + (isGzip ? ".gz" : ""))
                        .build());
                break;
            case "xml":
                headers.setContentType(MediaType.APPLICATION_XML);
                headers.setContentDisposition(ContentDisposition.attachment()
                        .filename("export.xml" + (isGzip ? ".gz" : ""))
                        .build());
                break;
            case "parquet":
                headers.setContentType(MediaType.parseMediaType("application/octet-stream"));
                headers.setContentDisposition(ContentDisposition.attachment()
                        .filename("export.parquet")
                        .build());
                break;
        }

        if (isGzip && !"parquet".equals(format)) {
            headers.add(HttpHeaders.CONTENT_ENCODING, "gzip");
        }

        StreamingResponseBody responseBody = outputStream -> {
            try {
                exportService.streamExport(job, outputStream);
            } catch (Exception e) {
                throw new RuntimeException("Error during export streaming", e);
            }
        };

        return ResponseEntity.ok()
                .headers(headers)
                .body(responseBody);
    }

    @GetMapping("/exports/benchmark")
    public ResponseEntity<?> runBenchmark() {
        try {
            Map<String, Object> results = exportService.executeBenchmark();
            return ResponseEntity.ok(results);
        } catch (Exception e) {
            Map<String, String> error = new LinkedHashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }
}
