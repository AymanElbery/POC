import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { HttpClientModule } from '@angular/common/http';
import { Subscription } from 'rxjs';
import { JobService, Job, JobUpdate } from './services/job.service';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, FormsModule, HttpClientModule],
  providers: [JobService],
  templateUrl: './app.html',
  styleUrls: ['./app.css']
})
export class App implements OnInit, OnDestroy {
  title = 'RabbitMQ POC Example';
  
  // Connection status
  isWebSocketConnected = false;
  
  // Job creation
  selectedJobType = 'data_processing';
  jobData: any = {
    data_processing: { steps: 10, delay: 1 },
    file_conversion: { filename: 'document.pdf', size: 2000 },
    report_generation: { type: 'monthly', sections: ['summary', 'details', 'charts', 'conclusion'] }
  };
  
  // Job tracking
  activeJobs: JobUpdate[] = [];
  completedJobs: JobUpdate[] = [];
  
  // Statistics
  queueStats: any = null;
  
  // Loading states
  isCreatingJob = false;
  isLoadingStats = false;
  
  private subscriptions: Subscription[] = [];

  constructor(private jobService: JobService) {}

  ngOnInit(): void {
    this.initializeSubscriptions();
    this.loadQueueStats();
  }

  ngOnDestroy(): void {
    this.subscriptions.forEach(sub => sub.unsubscribe());
    this.jobService.disconnect();
  }

  // TrackBy function for ngFor performance optimization
  trackByJobId(index: number, job: JobUpdate): string {
    return job.jobId;
  }

  private initializeSubscriptions(): void {
    // Subscribe to WebSocket connection status
    const connectionSub = this.jobService.getConnectionStatus().subscribe(
      (connected) => {
        this.isWebSocketConnected = connected;
        console.log(`WebSocket ${connected ? 'connected' : 'disconnected'}`);
      }
    );
    this.subscriptions.push(connectionSub);

    // Subscribe to job updates
    const jobUpdatesSub = this.jobService.getJobUpdates().subscribe(
      (update) => {
        this.handleJobUpdate(update);
      }
    );
    this.subscriptions.push(jobUpdatesSub);
  }

  private handleJobUpdate(update: JobUpdate): void {
    console.log('Received job update:', update);

    // Update active jobs list
    const existingIndex = this.activeJobs.findIndex(job => job.jobId === update.jobId);
    
    if (existingIndex >= 0) {
      this.activeJobs[existingIndex] = update;
    } else {
      this.activeJobs.push(update);
    }

    // Move completed/failed jobs to completed list
    if (update.status === 'completed' || update.status === 'failed') {
      this.moveJobToCompleted(update.jobId);
    }

    // Show notification for job completion
    if (update.status === 'completed') {
      this.showNotification(`Job ${update.jobId} completed successfully!`, 'success');
    } else if (update.status === 'failed') {
      this.showNotification(`Job ${update.jobId} failed: ${update.message}`, 'error');
    }
  }

  private removeDuplicatesCompleted(jobs: JobUpdate[]): JobUpdate[] {
    const uniqueJobs: { [key: string]: JobUpdate } = {}; // Use jobId as key
    jobs.forEach(job => {
      uniqueJobs[job.jobId] = job;
    });
    return Object.values(uniqueJobs);
  }

  private moveJobToCompleted(jobId: string): void {
    const jobIndex = this.activeJobs.findIndex(job => job.jobId === jobId);
    if (jobIndex >= 0) {
      const completedJob = this.activeJobs.splice(jobIndex, 1)[0]; 
      // check for duplicates before adding
      if (!this.completedJobs.find(job => job.jobId === completedJob.jobId)) {
        this.completedJobs.unshift(completedJob); // Add to beginning 
      }
      // Keep only last 10 completed jobs
      if (this.completedJobs.length > 10) {
        this.completedJobs = this.completedJobs.slice(0, 10);
      }
    }
  }

  createJob(): void {
    if (!this.isWebSocketConnected) {
      this.showNotification('WebSocket not connected. Please wait...', 'warning');
      return;
    }

    this.isCreatingJob = true;

    const job: Job = {
      type: this.selectedJobType,
      data: this.jobData[this.selectedJobType],
      priority: 1
    };

    const createSub = this.jobService.createJob(job).subscribe({
      next: (response) => {
        console.log('Job created:', response);
        this.showNotification(`Job ${response.jobId} created successfully!`, 'success');
        
        // Subscribe to job updates
        this.jobService.subscribeToJob(response.jobId);
        
        this.isCreatingJob = false;
      },
      error: (error) => {
        console.error('Error creating job:', error);
        this.showNotification('Failed to create job: ' + error.message, 'error');
        this.isCreatingJob = false;
      }
    });
    
    this.subscriptions.push(createSub);
  }

  loadQueueStats(): void {
    this.isLoadingStats = true;
    
    const statsSub = this.jobService.getQueueStats().subscribe({
      next: (stats) => {
        this.queueStats = stats;
        this.isLoadingStats = false;
      },
      error: (error) => {
        console.error('Error loading stats:', error);
        this.isLoadingStats = false;
      }
    });
    
    this.subscriptions.push(statsSub);
  }

  onJobTypeChange(): void {
    // Update form data when job type changes
    console.log('Selected job type:', this.selectedJobType);
  }

  getProgressBarColor(status: string): string {
    switch (status) {
      case 'completed': return '#4caf50';
      case 'failed': return '#f44336';
      case 'processing': return '#2196f3';
      default: return '#9e9e9e';
    }
  }

  getStatusIcon(status: string): string {
    switch (status) {
      case 'completed': return '✅';
      case 'failed': return '❌';
      case 'processing': return '⏳';
      default: return '❓';
    }
  }

  clearCompletedJobs(): void {
    this.completedJobs = [];
    this.jobService.clearCompletedJobs();
    this.showNotification('Completed jobs cleared', 'info');
  }

  testWebSocketConnection(): void {
    this.jobService.ping();
    this.jobService.getWebSocketStats();
  }

  reconnectWebSocket(): void {
    this.jobService.reconnect();
    this.showNotification('Attempting to reconnect...', 'info');
  }

  private showNotification(message: string, type: 'success' | 'error' | 'warning' | 'info'): void {
    // Simple notification implementation
    console.log(`[${type.toUpperCase()}] ${message}`);
    
    // You could integrate with a toast library here
    // For now, we'll just show an alert for important messages
    if (type === 'error') {
      alert(`Error: ${message}`);
    }
  }

  // Utility methods for template
  formatTimestamp(timestamp: string): string {
    return new Date(timestamp).toLocaleTimeString();
  }

  formatJobData(data: any): string {
    return JSON.stringify(data, null, 2);
  }

  getJobTypeDisplayName(type: string): string {
    const names: { [key: string]: string } = {
      'data_processing': 'Data Processing',
      'file_conversion': 'File Conversion',
      'report_generation': 'Report Generation'
    };
    return names[type] || type;
  }
}