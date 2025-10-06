import pika
import json
import time
import random
import logging
import sys
import os
from datetime import datetime
import signal

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class JobWorker:
    def __init__(self):
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:password@localhost:5672')
        self.connection = None
        self.channel = None
        self.should_stop = False
        
        # Queue names
        self.JOB_QUEUE = 'job_queue'
        self.RESULT_QUEUE = 'result_queue'
        
        # Worker configuration
        self.prefetch_count = 1
        self.max_retries = 3
        
    def connect(self):
        """Connect to RabbitMQ with retry logic"""
        max_retries = 10
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempting to connect to RabbitMQ (attempt {attempt + 1}/{max_retries})")
                
                # Parse connection parameters
                params = pika.URLParameters(self.rabbitmq_url)
                self.connection = pika.BlockingConnection(params)
                self.channel = self.connection.channel()
                
                # Declare queues
                self.channel.queue_declare(queue=self.JOB_QUEUE, durable=True)
                self.channel.queue_declare(queue=self.RESULT_QUEUE, durable=True)
                
                # Set QoS for fair dispatching
                self.channel.basic_qos(prefetch_count=self.prefetch_count)
                
                logger.info("✅ Connected to RabbitMQ successfully")
                return True
                
            except Exception as e:
                logger.error(f"❌ Failed to connect to RabbitMQ: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"⏳ Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error("❌ Max connection attempts reached")
                    return False
        
        return False
    
    def process_job(self, job_data):
        """Process a job and return progress updates"""
        job_id = job_data['id']
        job_type = job_data['type']
        data = job_data['data']
        
        logger.info(f"📋 Processing job {job_id} of type {job_type}")
        
        try:
            # Send initial progress
            self.send_progress(job_id, 0, "Job started", "processing")
            
            if job_type == 'data_processing':
                return self.process_data_job(job_id, data)
            elif job_type == 'file_conversion':
                return self.process_file_conversion(job_id, data)
            elif job_type == 'report_generation':
                return self.process_report_generation(job_id, data)
            else:
                raise ValueError(f"Unknown job type: {job_type}")
                
        except Exception as e:
            logger.error(f"❌ Error processing job {job_id}: {e}")
            self.send_progress(job_id, 0, f"Job failed: {str(e)}", "failed")
            raise
    
    def process_data_job(self, job_id, data):
        """Simulate data processing job"""
        total_steps = data.get('steps', 10)
        delay = data.get('delay', 1)
        
        for step in range(1, total_steps + 1):
            if self.should_stop:
                break
                
            # Simulate processing
            time.sleep(delay)
            
            progress = int((step / total_steps) * 100)
            message = f"Processing step {step}/{total_steps}"
            
            self.send_progress(job_id, progress, message, "processing")
            logger.info(f"📊 Job {job_id}: {message} ({progress}%)")
        
        # Job completed
        result = {
            'processed_items': total_steps,
            'processing_time': total_steps * delay,
            'status': 'completed'
        }
        
        self.send_progress(job_id, 100, "Job completed successfully", "completed", result)
        return result
    
    def process_file_conversion(self, job_id, data):
        """Simulate file conversion job"""
        file_name = data.get('filename', 'unknown.txt')
        file_size = data.get('size', 1000)
        
        # Simulate conversion with progress
        chunk_size = 100
        chunks = file_size // chunk_size
        
        for chunk in range(1, chunks + 1):
            if self.should_stop:
                break
                
            time.sleep(0.5)  # Simulate processing time
            
            progress = int((chunk / chunks) * 100)
            message = f"Converting {file_name}: chunk {chunk}/{chunks}"
            
            self.send_progress(job_id, progress, message, "processing")
        
        result = {
            'original_file': file_name,
            'converted_file': f"converted_{file_name}",
            'size': file_size,
            'status': 'completed'
        }
        
        self.send_progress(job_id, 100, f"File {file_name} converted successfully", "completed", result)
        return result
    
    def process_report_generation(self, job_id, data):
        """Simulate report generation job"""
        report_type = data.get('type', 'monthly')
        sections = data.get('sections', ['summary', 'details', 'charts', 'conclusion'])
        
        for i, section in enumerate(sections):
            if self.should_stop:
                break
                
            # Simulate section generation
            time.sleep(random.uniform(1, 3))
            
            progress = int(((i + 1) / len(sections)) * 100)
            message = f"Generating {section} section"
            
            self.send_progress(job_id, progress, message, "processing")
        
        result = {
            'report_type': report_type,
            'sections_generated': len(sections),
            'pages': random.randint(10, 50),
            'status': 'completed'
        }
        
        self.send_progress(job_id, 100, f"{report_type.title()} report generated successfully", "completed", result)
        return result
    
    def send_progress(self, job_id, progress, message, status, result=None):
        """Send progress update to result queue"""
        try:
            progress_data = {
                'jobId': job_id,
                'progress': progress,
                'message': message,
                'status': status,
                'timestamp': datetime.now().isoformat(),
                'result': result
            }

            self.channel.basic_publish(
                exchange='',
                routing_key=self.RESULT_QUEUE,
                body=json.dumps(progress_data),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    timestamp=int(time.time())
                )
            )
            
        except Exception as e:
            logger.error(f"❌ Failed to send progress update: {e}")
    
    def process_message(self, channel, method, properties, body):
        """Process incoming job message"""
        try:
            job_data = json.loads(body.decode('utf-8'))
            job_id = job_data['id']
            
            logger.info(f"📥 Received job: {job_id}")
            
            # Process the job
            result = self.process_job(job_data)
            
            # Acknowledge the message
            channel.basic_ack(delivery_tag=method.delivery_tag)
            logger.info(f"✅ Job {job_id} completed successfully")
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in message: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
        except Exception as e:
            logger.error(f"❌ Error processing message: {e}")
            
            # Check retry count
            retry_count = getattr(properties, 'headers', {}).get('x-retry-count', 0)
            
            if retry_count < self.max_retries:
                # Requeue with increased retry count
                headers = {'x-retry-count': retry_count + 1}
                channel.basic_publish(
                    exchange='',
                    routing_key=self.JOB_QUEUE,
                    body=body,
                    properties=pika.BasicProperties(headers=headers, delivery_mode=2)
                )
                logger.info(f"🔄 Requeued message (retry {retry_count + 1}/{self.max_retries})")
            else:
                logger.error(f"❌ Max retries reached, sending to DLQ")
            
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    
    def start_consuming(self):
        """Start consuming messages"""
        try:
            self.channel.basic_consume(
                queue=self.JOB_QUEUE,
                on_message_callback=self.process_message
            )
            
            logger.info("🔄 Worker started. Waiting for jobs...")
            logger.info("📊 Press CTRL+C to exit")
            
            self.channel.start_consuming()
            
        except KeyboardInterrupt:
            logger.info("📴 Stopping worker...")
            self.stop()
        except Exception as e:
            logger.error(f"❌ Error in consumer: {e}")
            raise
    
    def stop(self):
        """Stop the worker gracefully"""
        self.should_stop = True
        
        if self.channel:
            self.channel.stop_consuming()
            
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            
        logger.info("✅ Worker stopped gracefully")

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"📡 Received signal {signum}")
    worker.stop()
    sys.exit(0)

if __name__ == "__main__":
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    worker = JobWorker()
    
    if worker.connect():
        worker.start_consuming()
    else:
        logger.error("❌ Failed to start worker")
        sys.exit(1)