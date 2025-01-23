package com.duynguyen;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.duynguyen.utils.Log;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Base64;
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
        final BlockBlobClient blockBlobClient;
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
        }
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        try {
            // Expect: JSON format for upload initialization
            // {"action": "init", "fileName": "example.jpg", "contentType": "image/jpeg",
            // "size": 1234567, "container": "images"}
            if (message.contains("\"action\":\"init\"")) {
                handleUploadInit(conn, message);
            } else {
                sendError(conn, "Invalid message format");
            }
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

    private void handleUploadInit(WebSocket conn, String message) {
        try {
            // basic validation
            String[] parts = message.split(",");
            String fileName = parts[1].split(":")[1].replace("\"", "").trim();
            String contentType = parts[2].split(":")[1].replace("\"", "").trim();
            long size = Long.parseLong(parts[3].split(":")[1].trim());
            String container = parts[4].split(":")[1].replace("\"", "").replace("}", "").trim();

            Log.info("Initializing upload for file: " + fileName + ", size: " + size + ", container: " + container);

            BlobContainerClient containerClient = blobServiceClient.createBlobContainerIfNotExists(container);
            BlockBlobClient blockBlobClient = containerClient.getBlobClient(fileName)
                    .getBlockBlobClient();

            UploadSession session = new UploadSession(fileName, contentType, size, container, blockBlobClient);
            sessions.put(conn, session);

            conn.send("{\"status\":\"ready\",\"uploadId\":\"" + session.uploadId + "\"}");
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

        // If buffer reaches block size or it's the last chunk, upload it
        if (session.buffer.size() >= BLOCK_SIZE || session.bytesReceived >= session.totalSize) {
            uploadBlock(conn, session);
        }

        // Send progress update
        double progress = (double) session.bytesReceived / session.totalSize * 100;
        conn.send("{\"status\":\"progress\",\"percentage\":" + String.format("%.2f", progress) + "}");

        // If upload is complete, commit the blob
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
                    Log.info(String.format("Uploaded block %d for file %s", session.blockIndex, session.fileName));
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
            String[] blockIds = new String[session.blockIndex];
            for (int i = 0; i < session.blockIndex; i++) {
                blockIds[i] = Base64.getEncoder().encodeToString(
                        String.format("%06d", i).getBytes());
            }

            session.blockBlobClient.commitBlockList(java.util.Arrays.asList(blockIds));
            conn.send("{\"status\":\"complete\",\"url\":\"" + session.blockBlobClient.getBlobUrl() + "\"}");
            sessions.remove(conn);
        } catch (Exception e) {
            Log.error("Failed to commit blob: " + e.getMessage());
            sendError(conn, "Failed to commit blob: " + e.getMessage());
        }
    }

    private void sendError(WebSocket conn, String message) {
        conn.send("{\"status\":\"error\",\"message\":\"" + message + "\"}");
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