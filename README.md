# 🐰 RabbitMQ Angular Full-Stack Example

A complete end-to-end demonstration of asynchronous message processing using modern web technologies.

**Created by:** AymanElbery  
**Date:** October 6, 2025  
**Version:** 1.0.0

## 🏗️ Architecture Overview

This project demonstrates a real-world async processing pipeline with real-time updates:

[Angular App] → [Node.js API] → [RabbitMQ] → [Python Worker] → [WebSocket] → [Angular App]

### Components

- **🅰️ Angular 20 Frontend**: User interface with real-time job monitoring
- **🚀 Node.js Express API**: RESTful API and RabbitMQ message producer
- **🐍 Python Worker**: Background job processor and consumer
- **🔌 WebSocket Server**: Real-time communication via Socket.IO
- **🐰 RabbitMQ**: Message queue for reliable job processing

## ✨ Features

- ✅ Real-time job progress tracking with dynamic color-coded progress bars
- ✅ Multiple job types (Data Processing, File Conversion, Report Generation)
- ✅ Queue statistics monitoring with live metrics
- ✅ WebSocket connection status indicators
- ✅ Error handling and retry mechanisms
- ✅ Responsive UI with smooth animations
- ✅ Job history and completion tracking
- ✅ Connection health monitoring and auto-reconnection
- ✅ Scalable worker instances for high throughput

## 🛠️ Technology Stack

| Component | Technology | Version |
|-----------|------------|---------|
| Frontend | Angular | 20.x |
| Backend API | Node.js + Express | 18.x |
| Worker | Python | 3.11 |
| Message Queue | RabbitMQ | 3.x |
| WebSocket | Socket.IO | 4.x |
| Containerization | Docker | Latest |

## 📋 Prerequisites

- **Docker & Docker Compose** (for backend services)
- **Node.js 18+** (for Angular development)
- **Angular CLI 20+** (`npm install -g @angular/cli@20`)
- **Git** (for cloning)

## 🚀 Quick Start

### 1. Clone the Repository
```bash
git clone <repository-url>
cd rabbitmq-angular-poc
```
### 2. Start Backend Services (Docker)
 ```bash  
# Start RabbitMQ, API, Python Worker, and WebSocket server
docker-compose up --build

# Verify services are running
docker-compose ps
```
### 3. Start Angular Frontend (Local)
 ```bash 
# Navigate to Angular app
cd angular-app

# Install dependencies
npm install

# Start development server
ng serve --host 0.0.0.0 --port 4200 
```
### 3. Start Angular Frontend (Local)
🌐 Angular App: http://localhost:4200 

🐰 RabbitMQ Management: http://localhost:15672 (admin/password)

🔧 API Health: http://localhost:3001/health

📡 WebSocket Health: http://localhost:3002/health









