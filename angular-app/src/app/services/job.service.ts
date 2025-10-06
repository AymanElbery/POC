import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, BehaviorSubject, Subject } from 'rxjs';
import { io, Socket } from 'socket.io-client';

export interface Job {
  id?: string;
  type: string;
  data: any;
  priority?: number;
}

export interface JobUpdate {
  jobId: string;
  progress: number;
  message: string;
  status: 'processing' | 'completed' | 'failed';
  result?: any;
  timestamp: string;
}

export interface JobStatus {
  jobId: string;
  status: string;
  message: string;
}

@Injectable({
  providedIn: 'root'
})
export class JobService {
  private readonly API_URL = 'http://localhost:3001/api';
  private readonly WEBSOCKET_URL = 'http://localhost:3002';
  
  private socket!: Socket; // Use definite assignment assertion
  private connectionStatus$ = new BehaviorSubject<boolean>(false);
  private jobUpdates$ = new Subject<JobUpdate>();
  private activeJobs = new Map<string, JobUpdate>();

  constructor(private http: HttpClient) {
    this.initializeWebSocket();
  }

  private initializeWebSocket(): void {
    this.socket = io(this.WEBSOCKET_URL, {
      autoConnect: true,
      reconnection: true,
      reconnectionDelay: 1000,
      reconnectionAttempts: 5,
      timeout: 20000
    });

    // Connection events
    this.socket.on('connect', () => {
      console.log('✅ Connected to WebSocket server');
      this.connectionStatus$.next(true);
    });

    this.socket.on('disconnect', (reason: any) => {
      console.log('🔌 Disconnected from WebSocket server:', reason);
      this.connectionStatus$.next(false);
    });

    this.socket.on('connect_error', (error: any) => {
      console.error('❌ WebSocket connection error:', error);
      this.connectionStatus$.next(false);
    });

    // Job update events
    this.socket.on('job_update', (update: JobUpdate) => {
      console.log('📨 Job update received:', update);
      this.activeJobs.set(update.jobId, update);
      this.jobUpdates$.next(update);
    });

    this.socket.on('job_progress', (update: JobUpdate) => {
      console.log('📊 Job progress received:', update);
      this.activeJobs.set(update.jobId, update);
      this.jobUpdates$.next(update);
    });

    // Connection confirmation
    this.socket.on('connected', (data: any) => {
      console.log('🎉 WebSocket connection confirmed:', data);
    });

    // Subscription confirmations
    this.socket.on('subscribed', (data: any) => {
      console.log('📥 Subscribed to job:', data);
    });

    this.socket.on('unsubscribed', (data: any) => {
      console.log('📤 Unsubscribed from job:', data);
    });

    // Handle pong for connection health
    this.socket.on('pong', (data: any) => {
      console.log('🏓 Pong received:', data);
    });
  }

  // Create a new job
  createJob(job: Job): Observable<any> {
    return this.http.post(`${this.API_URL}/jobs`, job);
  }

  // Get job status from API
  getJobStatus(jobId: string): Observable<JobStatus> {
    return this.http.get<JobStatus>(`${this.API_URL}/jobs/${jobId}/status`);
  }

  // Get queue statistics
  getQueueStats(): Observable<any> {
    return this.http.get(`${this.API_URL}/stats`);
  }

  // Subscribe to a specific job's updates
  subscribeToJob(jobId: string): void {
    if (this.socket?.connected) {
      this.socket.emit('subscribe_job', jobId);
    } else {
      console.warn('⚠️ WebSocket not connected, cannot subscribe to job');
    }
  }

  // Unsubscribe from a specific job's updates
  unsubscribeFromJob(jobId: string): void {
    if (this.socket?.connected) {
      this.socket.emit('unsubscribe_job', jobId);
    }
  }

  // Get real-time job updates observable
  getJobUpdates(): Observable<JobUpdate> {
    return this.jobUpdates$.asObservable();
  }

  // Get connection status observable
  getConnectionStatus(): Observable<boolean> {
    return this.connectionStatus$.asObservable();
  }

  // Get current job status from local cache
  getCurrentJobStatus(jobId: string): JobUpdate | undefined {
    return this.activeJobs.get(jobId);
  }

  // Get all active jobs
  getAllActiveJobs(): JobUpdate[] {
    return Array.from(this.activeJobs.values());
  }

  // Send ping to test connection
  ping(): void {
    if (this.socket?.connected) {
      this.socket.emit('ping');
    }
  }

  // Get WebSocket connection stats
  getWebSocketStats(): void {
    if (this.socket?.connected) {
      this.socket.emit('get_stats');
      
      this.socket.once('stats', (stats: any) => {
        console.log('📊 WebSocket stats:', stats);
      });
    }
  }

  // Clear completed jobs from cache
  clearCompletedJobs(): void {
    for (const [jobId, job] of this.activeJobs.entries()) {
      if (job.status === 'completed' || job.status === 'failed') {
        this.activeJobs.delete(jobId);
      }
    }
  }

  // Disconnect WebSocket
  disconnect(): void {
    if (this.socket) {
      this.socket.disconnect();
      this.connectionStatus$.next(false);
    }
  }

  // Reconnect WebSocket
  reconnect(): void {
    if (this.socket) {
      this.socket.connect();
    } else {
      // Reinitialize if socket was destroyed
      this.initializeWebSocket();
    }
  }
}