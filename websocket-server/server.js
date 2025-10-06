const express = require('express');
const http = require('http');
const socketIO = require('socket.io');
const amqp = require('amqplib');
const cors = require('cors');

const app = express();
const server = http.createServer(app);

// Configure Socket.IO with CORS
const io = socketIO(server, {
  cors: {
    origin: ["http://localhost:4200", "http://localhost:3000"],
    methods: ["GET", "POST"],
    credentials: true
  }
});

const PORT = process.env.PORT || 3002;
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://admin:password@localhost:5672';

// Middleware
app.use(cors());
app.use(express.json());

// RabbitMQ Connection
let connection = null;
let channel = null;

const QUEUES = {
  RESULT_QUEUE: 'result_queue'
};

// Store active Socket.IO connections
const activeConnections = new Map();

async function connectRabbitMQ() {
  try {
    connection = await amqp.connect(RABBITMQ_URL);
    channel = await connection.createChannel();

    // Assert result queue
    await channel.assertQueue(QUEUES.RESULT_QUEUE, { durable: true });

    console.log('✅ WebSocket server connected to RabbitMQ');

    // Start consuming result messages
    await channel.consume(QUEUES.RESULT_QUEUE, (message) => {
      if (message) {
        try {
          const result = JSON.parse(message.content.toString());
          console.log('📨 Received result:', result.jobId, result.status);

          // Broadcast to all connected clients or specific job subscribers
          broadcastJobUpdate(result);

          // Acknowledge the message
          channel.ack(message);
        } catch (error) {
          console.error('❌ Error processing result message:', error);
          channel.nack(message, false, false);
        }
      }
    }, { noAck: false });

    // Handle connection events
    connection.on('error', (err) => {
      console.error('❌ RabbitMQ connection error:', err);
      setTimeout(connectRabbitMQ, 5000);
    });

    connection.on('close', () => {
      console.log('🔌 RabbitMQ connection closed');
      setTimeout(connectRabbitMQ, 5000);
    });

  } catch (error) {
    console.error('❌ Failed to connect to RabbitMQ:', error);
    setTimeout(connectRabbitMQ, 5000);
  }
}

function broadcastJobUpdate(jobUpdate) {
  const { jobId, progress, message, status, result, timestamp } = jobUpdate;

  // Broadcast to all clients
  io.emit('job_update', {
    jobId,
    progress,
    message,
    status,
    result,
    timestamp
  });

  // Also emit to specific job room if clients subscribed to specific jobs
  io.to(`job_${jobId}`).emit('job_progress', {
    jobId,
    progress,
    message,
    status,
    result,
    timestamp
  });

  console.log(`📡 Broadcasted update for job ${jobId}: ${status} (${progress}%)`);
}

// Socket.IO connection handling
io.on('connection', (socket) => {
  console.log(`🔌 Client connected: ${socket.id}`);
  
  // Store connection info
  activeConnections.set(socket.id, {
    connectedAt: new Date(),
    subscribedJobs: new Set()
  });

  // Send connection confirmation
  socket.emit('connected', {
    socketId: socket.id,
    timestamp: new Date().toISOString(),
    message: 'Connected to WebSocket server'
  });

  // Handle job subscription
  socket.on('subscribe_job', (jobId) => {
    console.log(`📥 Client ${socket.id} subscribed to job ${jobId}`);
    socket.join(`job_${jobId}`);
    
    const connectionInfo = activeConnections.get(socket.id);
    if (connectionInfo) {
      connectionInfo.subscribedJobs.add(jobId);
    }

    socket.emit('subscribed', { jobId, message: `Subscribed to job ${jobId}` });
  });

  // Handle job unsubscription
  socket.on('unsubscribe_job', (jobId) => {
    console.log(`📤 Client ${socket.id} unsubscribed from job ${jobId}`);
    socket.leave(`job_${jobId}`);
    
    const connectionInfo = activeConnections.get(socket.id);
    if (connectionInfo) {
      connectionInfo.subscribedJobs.delete(jobId);
    }

    socket.emit('unsubscribed', { jobId, message: `Unsubscribed from job ${jobId}` });
  });

  // Handle ping for connection health
  socket.on('ping', () => {
    socket.emit('pong', { timestamp: new Date().toISOString() });
  });

  // Handle client requests for connection stats
  socket.on('get_stats', () => {
    socket.emit('stats', {
      connectedClients: activeConnections.size,
      yourSubscriptions: Array.from(activeConnections.get(socket.id)?.subscribedJobs || []),
      serverUptime: process.uptime(),
      timestamp: new Date().toISOString()
    });
  });

  // Handle disconnection
  socket.on('disconnect', (reason) => {
    console.log(`🔌 Client disconnected: ${socket.id} (${reason})`);
    
    // Clean up
    activeConnections.delete(socket.id);
    
    // Leave all job rooms
    const rooms = Array.from(socket.rooms);
    rooms.forEach(room => {
      if (room.startsWith('job_')) {
        socket.leave(room);
      }
    });
  });

  // Handle errors
  socket.on('error', (error) => {
    console.error(`❌ Socket error from ${socket.id}:`, error);
  });
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({
    status: 'OK',
    timestamp: new Date().toISOString(),
    connectedClients: activeConnections.size,
    rabbitmq: channel ? 'connected' : 'disconnected',
    uptime: process.uptime()
  });
});

// Get connection stats
app.get('/api/stats', (req, res) => {
  const stats = {
    connectedClients: activeConnections.size,
    uptime: process.uptime(),
    timestamp: new Date().toISOString(),
    rabbitmq: channel ? 'connected' : 'disconnected'
  };

  res.json(stats);
});

// Start server
server.listen(PORT, () => {
  console.log(`🚀 WebSocket server running on port ${PORT}`);
  console.log(`📡 Socket.IO server ready for connections`);
});

// Initialize RabbitMQ connection
connectRabbitMQ();

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('📴 Shutting down WebSocket server...');
  
  // Close all socket connections
  io.close();
  
  // Close RabbitMQ connection
  if (channel) await channel.close();
  if (connection) await connection.close();
  
  // Close HTTP server
  server.close(() => {
    console.log('✅ WebSocket server shut down gracefully');
    process.exit(0);
  });
});

// Handle unhandled rejections
process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
});