package com.duynguyen;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.duynguyen.utils.Log;
import org.apache.commons.io.FilenameUtils;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StorageWebSocketServer extends WebSocketServer {
    private final ConcurrentHashMap<WebSocket, UploadSession> sessions = new ConcurrentHashMap<>();
    private final BlobServiceClient blobServiceClient;
    private final ExecutorService uploadExecutor;
    private static final int BLOCK_SIZE = 4 * 1024 * 1024;

    public StorageWebSocketServer(int port, String azureConnectionString) {
        super(new InetSocketAddress(port));
        this.blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(azureConnectionString)
                .buildClient();
        this.uploadExecutor = Executors.newFixedThreadPool(10);
    }

    static class UploadSession {
        final String fileName;
        final String contentType;
        final long totalSize;
        final String containerName;
        final ByteArrayOutputStream buffer;
        BlockBlobClient blockBlobClient;
        final String uploadId;
        int blockIndex;
        long bytesReceived;

        UploadSession(String fileName, String contentType, long totalSize, String containerName,
                      BlockBlobClient blockBlobClient) {
            this.fileName = fileName;
            this.contentType = contentType;
            this.totalSize = totalSize;
            this.containerName = containerName;
            this.buffer = new ByteArrayOutputStream(BLOCK_SIZE);
            this.blockBlobClient = blockBlobClient;
            this.uploadId = UUID.randomUUID().toString();
            this.blockIndex = 0;
            this.bytesReceived = 0;
        }
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        Log.info("New connection established: " + conn.getRemoteSocketAddress());
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        UploadSession session = sessions.remove(conn);
        if (session != null) {
            Log.info("Upload session closed for file: " + session.fileName + ", reason: " + reason);
            session.buffer.reset();
        } else {
            Log.info("Connection closed: " + conn.getRemoteSocketAddress());
        }
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        try {
            JSONObject json = new JSONObject(message);
            String action = json.getString("action");
            switch (action) {
                case "init":
                    handleUploadInit(conn, json);
                    break;
                case "resolve_blob_exists":
                    String strategy = json.getString("strategy");
                    handleBlobExistsStrategy(conn, strategy);
                    break;
                default:
                    sendError(conn, "Invalid action: " + action);
            }
        } catch (JSONException e) {
            Log.error("Invalid JSON: " + e.getMessage());
            sendError(conn, "Invalid message format");
        } catch (Exception e) {
            Log.error("Error processing message: " + e.getMessage());
            sendError(conn, "Failed to process message: " + e.getMessage());
        }
    }

    @Override
    public void onMessage(WebSocket conn, ByteBuffer buffer) {
        UploadSession session = sessions.get(conn);
        if (session == null) {
            sendError(conn, "No active upload session");
            return;
        }

        try {
            handleFileChunk(conn, session, buffer);
        } catch (Exception e) {
            Log.error("Error processing file chunk: " + e.getMessage());
            sendError(conn, "Failed to process file chunk: " + e.getMessage());
            sessions.remove(conn);
        }
    }

    private void handleUploadInit(WebSocket conn, JSONObject json) {
        try {
            String fileName = json.getString("fileName");
            String contentType = json.getString("contentType");
            long size = json.getLong("size");
            String container = json.getString("container");
            Log.info("Initializing upload for file: " + fileName + ", size: " + size + ", container: " + container);

            BlobContainerClient containerClient = blobServiceClient.createBlobContainerIfNotExists(container);
            BlockBlobClient blockBlobClient = containerClient.getBlobClient(fileName)
                    .getBlockBlobClient();

            UploadSession session = new UploadSession(fileName, contentType, size, container, blockBlobClient);
            sessions.put(conn, session);

            JSONObject response = new JSONObject();
            response.put("uploadId", session.uploadId);
            response.put("status", "ready");
            conn.send(response.toString());
        } catch (Exception e) {
            Log.error("Failed to initialize upload: " + e.getMessage());
            sendError(conn, "Failed to initialize upload: " + e.getMessage());
        }
    }

    private void handleFileChunk(WebSocket conn, UploadSession session, ByteBuffer buffer) {
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        session.buffer.write(data, 0, data.length);
        session.bytesReceived += data.length;
        if (session.buffer.size() >= BLOCK_SIZE || session.bytesReceived >= session.totalSize) {
            uploadBlock(conn, session);
        }
        double progress = (double) session.bytesReceived / session.totalSize * 100;
        JSONObject progressResponse = new JSONObject();
        progressResponse.put("percentage", String.format("%.2f", progress));
        progressResponse.put("status", "progress");
        conn.send(progressResponse.toString());
        if (session.bytesReceived >= session.totalSize) {
            commitBlob(conn, session);
        }
    }

    private void uploadBlock(WebSocket conn, UploadSession session) {
        try {
            byte[] blockData = session.buffer.toByteArray();
            String blockId = Base64.getEncoder().encodeToString(
                    String.format("%06d", session.blockIndex).getBytes());

            uploadExecutor.submit(() -> {
                try {
                    session.blockBlobClient.stageBlock(blockId, new ByteArrayInputStream(blockData), blockData.length);
                } catch (Exception e) {
                    Log.error("Failed to upload block: " + e.getMessage());
                    sendError(conn, "Failed to upload block: " + e.getMessage());
                }
            });

            session.blockIndex++;
            session.buffer.reset();
        } catch (Exception e) {
            Log.error("Error in uploadBlock: " + e.getMessage());
            sendError(conn, "Failed to upload block: " + e.getMessage());
        }
    }

    private void commitBlob(WebSocket conn, UploadSession session) {
        try {
            List<String> blockIds = getBlockIds(session);
            if (blockIds.isEmpty()) {
                sendError(conn, "No blocks to upload");
                return;
            }

            try {
                session.blockBlobClient.commitBlockList(blockIds);
            } catch (RuntimeException e) {
                if (e.getMessage() != null && e.getMessage().contains("BlobAlreadyExists")) {
                    JSONObject strategyPrompt = new JSONObject();
                    strategyPrompt.put("status", "blob_exists");
                    strategyPrompt.put("fileName", session.fileName);
                    strategyPrompt.put("existingBlobUrl", session.blockBlobClient.getBlobUrl());
                    strategyPrompt.put("strategies", new JSONArray(new String[]{"override", "uniquename"}));
                    conn.send(strategyPrompt.toString());
                    return;
                } else {
                    throw e;
                }
            }
            sendCompletionResponse(conn, session);
        } catch (Exception e) {
            Log.error("Failed to commit blob: " + e.getMessage());
            sendError(conn, "Failed to commit blob: " + e.getMessage());
        }
    }

    public void handleBlobExistsStrategy(WebSocket conn, String strategy) {
        UploadSession session = sessions.get(conn);
        if (session == null) {
            sendError(conn, "No active upload session");
            return;
        }

        try {
            List<String> blockIds = getBlockIds(session);
            if (blockIds.isEmpty()) {
                sendError(conn, "No blocks to commit");
                return;
            }
            switch (strategy) {
                case "override":
                    session.blockBlobClient.delete();
                    session.blockBlobClient.commitBlockList(blockIds);
                    break;
                case "uniquename":
                    String uniqueFileName = generateUniqueFileName(session.fileName);
                    session.blockBlobClient = session.blockBlobClient.getContainerClient()
                            .getBlobClient(uniqueFileName)
                            .getBlockBlobClient();
                    try {
                        session.blockBlobClient.commitBlockList(blockIds);
                    } catch (Exception e) {
                        sendError(conn, "Failed to commit blob: " + e.getMessage());
                        return;
                    }
                    break;
                default:
                    sendError(conn, "Invalid strategy");
                    return;
            }
            sendCompletionResponse(conn, session);
        } catch (Exception e) {
            sendError(conn, "Strategy execution failed: " + e.getMessage());
        }
    }

    private String generateUniqueFileName(String originalFileName) {
        String baseName = FilenameUtils.getBaseName(originalFileName);
        String extension = FilenameUtils.getExtension(originalFileName);
        return String.format("%s_%s.%s",
                baseName,
                UUID.randomUUID().toString().substring(0, 8),
                extension
        );
    }

    private List<String> getBlockIds(UploadSession session) {
        List<String> blockIds = new ArrayList<>();
        for (int i = 0; i < session.blockIndex; i++) {
            blockIds.add(Base64.getEncoder().encodeToString(
                    String.format("%06d", i).getBytes()));
        }
        return blockIds;
    }

    private void sendCompletionResponse(WebSocket conn, UploadSession session) {
        JSONObject completionResponse = new JSONObject();
        completionResponse.put("status", "complete");
        completionResponse.put("url", session.blockBlobClient.getBlobUrl());
        conn.send(completionResponse.toString());
        sessions.remove(conn);
    }

    private void sendError(WebSocket conn, String message) {
        JSONObject errorResponse = new JSONObject();
        errorResponse.put("message", message);
        errorResponse.put("status", "error");
        conn.send(errorResponse.toString());
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        Log.error("WebSocket error: " + ex.getMessage());
        if (conn != null) {
            sendError(conn, "Internal server error");
            sessions.remove(conn);
        }
    }

    @Override
    public void onStart() {
        Log.info("WebSocket server started on port " + getPort());
    }

    public void shutdown() throws InterruptedException {
        uploadExecutor.shutdown();
        try {
            uploadExecutor.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Log.error("Error shutting down upload executor: " + e.getMessage());
        }
        stop();
    }
}