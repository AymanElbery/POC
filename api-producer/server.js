const express = require('express');
const amqp = require('amqplib');
const cors = require('cors');
const helmet = require('helmet');
const { v4: uuidv4 } = require('uuid');

const app = express();
const PORT = process.env.PORT || 3001;
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://admin:password@localhost:5672';

// Middleware
app.use(helmet());
app.use(cors());
app.use(express.json());

// RabbitMQ Connection
let connection = null;
let channel = null;

const QUEUES = {
  JOB_QUEUE: 'job_queue',
  RESULT_QUEUE: 'result_queue'
};

async function connectRabbitMQ() {
  try {
    connection = await amqp.connect(RABBITMQ_URL);
    channel = await connection.createChannel();

    // Create Dead Letter Exchange first
    await channel.assertExchange('dlx', 'direct', { durable: true });
    await channel.assertQueue('failed_jobs', { durable: true });
    await channel.bindQueue('failed_jobs', 'dlx', 'failed');

    // Try to assert queues, if they exist with different config, delete and recreate
    try {
      await channel.assertQueue(QUEUES.JOB_QUEUE, {
        durable: true,
        arguments: {
          'x-dead-letter-exchange': 'dlx',
          'x-dead-letter-routing-key': 'failed'
        }
      });
    } catch (error) {
      if (error.message.includes('PRECONDITION_FAILED')) {
        console.log('⚠️ Queue exists with different config, deleting and recreating...');
        try {
          await channel.deleteQueue(QUEUES.JOB_QUEUE);
          await channel.assertQueue(QUEUES.JOB_QUEUE, {
            durable: true,
            arguments: {
              'x-dead-letter-exchange': 'dlx',
              'x-dead-letter-routing-key': 'failed'
            }
          });
        } catch (deleteError) {
          console.log('⚠️ Could not delete queue, creating without DLX...');
          await channel.assertQueue(QUEUES.JOB_QUEUE, { durable: true });
        }
      } else {
        throw error;
      }
    }

    // Assert result queue (simpler, no DLX needed)
    await channel.assertQueue(QUEUES.RESULT_QUEUE, {
      durable: true
    });

    console.log('✅ Connected to RabbitMQ');

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

// Initialize RabbitMQ connection
connectRabbitMQ();

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({
    status: 'OK',
    timestamp: new Date().toISOString(),
    rabbitmq: channel ? 'connected' : 'disconnected'
  });
});

// Create a new job
app.post('/api/jobs', async (req, res) => {
  try {
    if (!channel) {
      return res.status(503).json({ error: 'RabbitMQ not connected' });
    }

    const { type, data, priority = 1 } = req.body;

    if (!type || !data) {
      return res.status(400).json({ error: 'Type and data are required' });
    }

    const jobId = uuidv4();
    const job = {
      id: jobId,
      type,
      data,
      priority,
      createdAt: new Date().toISOString(),
      retryCount: 0,
      maxRetries: 3
    };

    // Send job to queue with persistence
    await channel.sendToQueue(
      QUEUES.JOB_QUEUE,
      Buffer.from(JSON.stringify(job)),
      {
        persistent: true,
        priority: priority,
        messageId: jobId,
        timestamp: Date.now()
      }
    );

    console.log(`📤 Job enqueued: ${jobId}`);

    res.status(201).json({
      success: true,
      jobId,
      message: 'Job created successfully'
    });

  } catch (error) {
    console.error('❌ Error creating job:', error);
    res.status(500).json({ error: 'Failed to create job' });
  }
});

// Get job status (for demonstration)
app.get('/api/jobs/:jobId/status', (req, res) => {
  // In production, you'd store job status in a database
  res.json({
    jobId: req.params.jobId,
    status: 'processing',
    message: 'Job is being processed'
  });
});

// Get queue stats
app.get('/api/stats', async (req, res) => {
  try {
    if (!channel) {
      return res.status(503).json({ error: 'RabbitMQ not connected' });
    }

    const jobQueue = await channel.checkQueue(QUEUES.JOB_QUEUE);
    const resultQueue = await channel.checkQueue(QUEUES.RESULT_QUEUE);

    res.json({
      jobQueue: {
        messageCount: jobQueue.messageCount,
        consumerCount: jobQueue.consumerCount
      },
      resultQueue: {
        messageCount: resultQueue.messageCount,
        consumerCount: resultQueue.consumerCount
      }
    });
  } catch (error) {
    console.error('❌ Error getting stats:', error);
    res.status(500).json({ error: 'Failed to get stats' });
  }
});

app.listen(PORT, () => {
  console.log(`🚀 API Producer server running on port ${PORT}`);
});

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('📴 Shutting down API Producer...');
  if (channel) await channel.close();
  if (connection) await connection.close();
  process.exit(0);
});