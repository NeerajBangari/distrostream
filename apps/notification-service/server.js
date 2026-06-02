const express = require('express');
const { Kafka } = require('kafkajs');
const WebSocket = require('ws');
const http = require('http');

const app = express();
const server = http.createServer(app);

// 1. Initialize WebSocket Server attached to our HTTP Server
const wss = new WebSocket.Server({ server, path: '/api/v1/notifications/connect' });

const KAFKA_BROKER = process.env.KAFKA_BOOTSTRAP_SERVERS || '127.0.0.1:9092';
const TOPIC = 'processed-stream';

// In-memory map to keep track of active WebSocket connections
// In a real app, you would map token/user_id to sockets
const activeConnections = new Set();

wss.on('connection', (ws) => {
    console.log('🔌 Client connected to Real-Time Notification Socket Channel');
    activeConnections.add(ws);

    ws.on('close', () => {
        console.log('❌ Client disconnected from Socket');
        activeConnections.delete(ws);
    });
});

// 2. Configure Kafka Consumer
const kafka = new Kafka({
    clientId: 'notification-service',
    brokers: [KAFKA_BROKER]
});
const consumer = kafka.consumer({ groupId: 'notification-group' });

const runPipelineConsumer = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC, fromBeginning: false });
    console.log(`🎧 Node.js Notification Service is listening to Kafka topic [${TOPIC}]...`);

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const rawData = message.value.toString();
            console.log(`⚡ Received processing success event from Kafka: ${rawData}`);
            
            const eventPayload = JSON.parse(rawData);

            // Broadcast the completion details to EVERY connected browser window
            activeConnections.forEach((client) => {
                if (client.readyState === WebSocket.OPEN) {
                    client.send(JSON.stringify({
                        event: 'VIDEO_TRANSCODING_COMPLETE',
                        data: eventPayload
                    }));
                }
            });
            console.log(`📢 Broadcasted notification alert to ${activeConnections.size} connected client sockets.`);
        },
    });
};

// Start both HTTP/WS server and Kafka loop
const PORT = 8080;
server.listen(PORT, () => {
    console.log(`🚀 Notification Gateway Server running on port ${PORT}`);
    runPipelineConsumer().catch(console.error);
});