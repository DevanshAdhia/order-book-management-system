# 📚 Order Book Management System

A high-performance **C++17 Order Book Management System** that simulates a modern electronic exchange. The application processes buy and sell orders using **price-time priority**, maintains an in-memory order book, performs concurrent order processing, and provides real-time market statistics with optional PostgreSQL persistence.

---

## 🚀 Features

* Multi-threaded order processing
* In-memory order book implementation
* Price-Time Priority matching algorithm
* Buy and Sell order management
* Market, Limit, and Cancel order support
* Thread-safe producer-consumer architecture
* Low-latency order execution
* Real-time best Bid/Ask calculation
* Order book depth tracking
* PostgreSQL integration for persistent storage
* Performance monitoring and latency measurement
* Timestamped logging system
* STL-based optimized data structures

---

## 🛠 Technologies Used

* C++17
* Standard Template Library (STL)
* Multithreading
* Mutex & Condition Variables
* Atomic Operations
* Producer-Consumer Pattern
* Thread-safe Queue
* PostgreSQL (libpq)
* Performance Optimization
* Data Structures & Algorithms

---

## ⚙️ System Workflow

```
Client Orders
      │
      ▼
Producer Threads
      │
      ▼
Thread-Safe Queue
      │
      ▼
Consumer Threads
      │
      ▼
Order Validation
      │
      ▼
Price-Time Priority Matching
      │
      ▼
Update Order Book
      │
      ▼
Best Bid / Best Ask
      │
      ▼
Market Statistics
      │
      ▼
PostgreSQL Database
```

---

## 📊 Order Matching Engine

The application maintains an in-memory order book where incoming orders are processed according to **Price-Time Priority**.

Processing includes:

* Order Validation
* Buy Order Processing
* Sell Order Processing
* Order Matching
* Partial Fill Handling
* Complete Execution
* Order Cancellation
* Order Modification
* Best Bid/Ask Updates
* Order Book Depth Calculation

---

## ⚡ Performance Optimizations

* Thread-safe queue implementation
* Reduced synchronization overhead
* Efficient STL containers
* Low memory allocation
* Optimized lookup operations
* Concurrent order processing
* Atomic counters
* Minimal lock contention
* Fast order matching
* Cache-friendly data structures

---

## 📈 Performance Metrics

The application monitors:

* Orders Received
* Orders Processed
* Queue Size
* Processing Throughput
* Average Latency
* Maximum Latency
* Order Execution Time
* Matching Efficiency

---

## 🗄 Database Support

The project supports PostgreSQL for storing processed order information.

Stored data includes:

* Exchange
* Symbol
* Order Type
* Price
* Quantity
* Timestamp
* Execution Status

---

## ▶️ Build

### Linux

```bash
g++ -std=c++17 main.cpp -lpthread -lpq -o OrderBook
```

### Run

```bash
./OrderBook
```

---

## 📌 Future Improvements

* NASDAQ ITCH Feed Support
* FIX Protocol Integration
* Level-2 Market Data
* Market Replay Engine
* Risk Management Module
* WebSocket Streaming
* REST API
* GUI Dashboard
* Distributed Matching Engine
* Docker Deployment

---

## 📖 Topics

* C++
* C++17
* Order Book
* Matching Engine
* Price-Time Priority
* Trading System
* Electronic Exchange
* High Performance Computing
* Low Latency
* Multithreading
* Concurrency
* Thread-safe Programming
* Producer Consumer
* Mutex
* Atomic Operations
* STL
* Data Structures
* Algorithms
* PostgreSQL
* libpq
* Financial Engineering
* Quantitative Finance
* Market Data
* Capital Markets
* Systems Programming
* Performance Optimization

---

## 👨‍💻 Author

**Devansh Adhia**

Backend Developer | C++ Developer | Financial Systems Enthusiast

Interested in:

* Low Latency Systems
* Trading Infrastructure
* Market Data Processing
* High Performance C++
* Concurrent Programming
* Quantitative Finance

---

## ⭐ If you found this project useful, consider giving it a star.
