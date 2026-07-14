#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <deque>
#include <future>
#include <iomanip>
#include <iostream>
#include <libpq-fe.h>
#include <map>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <optional>
#include <queue>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

using namespace std::chrono_literals;

// ----------------------------------------------------------------------------
// Domain enums (replace ad-hoc strings/bools used in the original code so the
// compiler enforces valid states instead of relying on string comparisons).
// ----------------------------------------------------------------------------

enum class Side : std::uint8_t { Buy, Sell };
enum class OrderType : std::uint8_t { Limit, Market };
enum class TimeInForce : std::uint8_t { GTC, IOC, FOK };
enum class OrderStatus : std::uint8_t { New, PartiallyFilled, Filled, Cancelled, Rejected };

// Self-trade prevention policy applied when the resting and aggressing order
// belong to the same logical client. Real venues offer several variants;
// we implement the three most common.
enum class SelfTradePolicy : std::uint8_t { CancelResting, CancelAggressor, CancelBoth };

inline const char* toString(Side s) { return s == Side::Buy ? "BUY" : "SELL"; }
inline const char* toString(OrderType t) { return t == OrderType::Market ? "MARKET" : "LIMIT"; }
inline const char* toString(TimeInForce t) {
    switch (t) {
        case TimeInForce::GTC: return "GTC";
        case TimeInForce::IOC: return "IOC";
        case TimeInForce::FOK: return "FOK";
    }
    return "GTC";
}
inline const char* toString(OrderStatus s) {
    switch (s) {
        case OrderStatus::New: return "NEW";
        case OrderStatus::PartiallyFilled: return "PARTIALLY_FILLED";
        case OrderStatus::Filled: return "FILLED";
        case OrderStatus::Cancelled: return "CANCELLED";
        case OrderStatus::Rejected: return "REJECTED";
    }
    return "NEW";
}

// ----------------------------------------------------------------------------
// Core data structures
// ----------------------------------------------------------------------------

// Represents a single resting/working order in the book.
// Stored BY VALUE inside an unordered_map (see OrderBook::orders_) so there is
// exactly one heap allocation per order (the map node) instead of a
// unique_ptr<Order> plus a separately-allocated Order.
struct Order {
    std::uint64_t id = 0;
    std::string clientOrderId;
    std::string clientId;   // logical owner: used for auth, dup-id scoping, STP
    std::string clientIp;
    std::string symbol;
    Side side = Side::Buy;
    OrderType type = OrderType::Limit;
    TimeInForce tif = TimeInForce::GTC;
    double price = 0.0;
    int quantity = 0;          // remaining (unfilled) quantity
    int originalQuantity = 0;  // quantity at acceptance time
    std::uint64_t sequence = 0; // monotonic insertion sequence -> FIFO tie-break
    bool active = true;
    OrderStatus status = OrderStatus::New;
};

struct TradeRecord {
    std::uint64_t tradeId = 0;
    std::uint64_t buyOrderId = 0;
    std::uint64_t sellOrderId = 0;
    std::string symbol;
    double price = 0.0;
    int quantity = 0;
    std::string timestamp;
};

struct ValidationResult {
    bool ok = true;
    std::string error;
};

struct OrderExecutionResult {
    bool accepted = false;
    std::uint64_t orderId = 0;
    std::string response;
};

// ----------------------------------------------------------------------------
// RAII wrappers for libpq resources. PQclear/PQfinish must always be called
// exactly once; wrapping them in unique_ptr with a custom deleter makes that
// guarantee automatic even when exceptions or early returns are involved.
// ----------------------------------------------------------------------------

struct PGResultDeleter {
    void operator()(PGresult* result) const noexcept {
        if (result != nullptr) {
            PQclear(result);
        }
    }
};
using PGResultPtr = std::unique_ptr<PGresult, PGResultDeleter>;

struct PGConnDeleter {
    void operator()(PGconn* conn) const noexcept {
        if (conn != nullptr) {
            PQfinish(conn);
        }
    }
};
using PGConnPtr = std::unique_ptr<PGconn, PGConnDeleter>;

// ----------------------------------------------------------------------------
// Thread-safe structured logger.
// A single mutex serializes writes to stdout so concurrent producers (TCP
// threads, worker threads, persistence thread) never interleave a line.
// Every line carries a timestamp, level, thread id and component tag so log
// output can be correlated across threads in production.
// ----------------------------------------------------------------------------

class ThreadSafeLogger {
public:
    enum class Level { INFO, WARNING, ERROR };

    void logClientConnected(const std::string& clientIp) {
        log(Level::INFO, "tcp", "client_connected ip=" + clientIp);
    }

    void logClientDisconnected(const std::string& clientIp) {
        log(Level::INFO, "tcp", "client_disconnected ip=" + clientIp);
    }

    void logOrderReceived(const std::string& message, const std::string& clientIp) {
        log(Level::INFO, "tcp", "order_received ip=" + clientIp + " payload=" + message);
    }

    void logValidationFailure(const std::string& reason, const std::string& clientIp) {
        log(Level::WARNING, "validation", "rejected ip=" + clientIp + " reason=" + reason);
    }

    void logOrderAdded(const Order& order) {
        std::ostringstream stream;
        stream << "order_id=" << order.id << " client_order_id=" << order.clientOrderId
               << " client_id=" << order.clientId << " symbol=" << order.symbol
               << " side=" << ::toString(order.side) << " type=" << ::toString(order.type)
               << " tif=" << ::toString(order.tif) << " price=" << order.price
               << " qty=" << order.quantity << " status=" << ::toString(order.status)
               << " seq=" << order.sequence;
        log(Level::INFO, "orderbook", "order_added " + stream.str());
    }

    void logOrderModified(const Order& order) {
        std::ostringstream stream;
        stream << "order_id=" << order.id << " client_order_id=" << order.clientOrderId
               << " price=" << order.price << " qty=" << order.quantity
               << " status=" << ::toString(order.status);
        log(Level::INFO, "orderbook", "order_modified " + stream.str());
    }

    void logOrderCancelled(const Order& order) {
        std::ostringstream stream;
        stream << "order_id=" << order.id << " client_order_id=" << order.clientOrderId
               << " reason_status=" << ::toString(order.status);
        log(Level::INFO, "orderbook", "order_cancelled " + stream.str());
    }

    void logTradeExecuted(const TradeRecord& trade, const std::string& detail = "") {
        std::ostringstream stream;
        stream << "trade_id=" << trade.tradeId << " buy_order=" << trade.buyOrderId
               << " sell_order=" << trade.sellOrderId << " symbol=" << trade.symbol
               << " price=" << trade.price << " qty=" << trade.quantity;
        if (!detail.empty()) {
            stream << " " << detail;
        }
        log(Level::INFO, "trade", "trade_executed " + stream.str());
    }

    void logPostgres(const std::string& action, const std::string& detail) {
        log(Level::INFO, "postgres", action + " " + detail);
    }

    void logReconnectAttempt(const std::string& detail) {
        log(Level::WARNING, "postgres", "reconnect_attempt " + detail);
    }

    void logError(const std::string& component, const std::string& message) {
        log(Level::ERROR, component, message);
    }

    void logWarning(const std::string& component, const std::string& message) {
        log(Level::WARNING, component, message);
    }

    void logShutdown(const std::string& message) {
        log(Level::INFO, "server", message);
    }

    void logLatency(const std::string& component, std::chrono::microseconds latency) {
        log(Level::INFO, component, "latency_us=" + std::to_string(latency.count()));
    }

private:
    static const char* toString(Level level) {
        switch (level) {
            case Level::INFO: return "INFO";
            case Level::WARNING: return "WARNING";
            case Level::ERROR: return "ERROR";
        }
        return "INFO";
    }

    void log(Level level, const std::string& component, const std::string& message) {
        const auto now = std::chrono::system_clock::now();
        const auto time = std::chrono::system_clock::to_time_t(now);
        const auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
        std::tm localTime{};
        localtime_r(&time, &localTime);
        std::ostringstream stamp;
        stamp << std::put_time(&localTime, "%Y-%m-%d %H:%M:%S") << '.' << std::setw(3) << std::setfill('0') << millis.count();
        std::ostringstream tid;
        tid << std::this_thread::get_id();

        // Build the full line before taking the lock so the critical section
        // is just the actual write, not string formatting.
        std::ostringstream line;
        line << "[" << stamp.str() << "] [" << toString(level) << "] [tid=" << tid.str() << "] [" << component << "] " << message;

        std::lock_guard<std::mutex> lock(mutex_);
        std::cout << line.str() << std::endl;
    }

    std::mutex mutex_;
};

// ----------------------------------------------------------------------------
// Thread-safe blocking/bounded-wait queue used both for the TCP->worker
// pipeline and for the persistence event pipeline.
// ----------------------------------------------------------------------------

template <typename T>
class ThreadSafeQueue {
public:
    template <typename U>
    void push(U&& value) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            queue_.push(std::forward<U>(value));
        }
        condition_.notify_one();
    }

    // Blocks until an item is available or the queue is shut down.
    // Returns false only when shut down AND empty (i.e. no more work ever).
    bool pop(T& value) {
        std::unique_lock<std::mutex> lock(mutex_);
        condition_.wait(lock, [this] { return shutdown_ || !queue_.empty(); });
        if (queue_.empty()) {
            return false; // shutdown_ must be true here
        }
        value = std::move(queue_.front());
        queue_.pop();
        return true;
    }

    // Waits up to `timeout` for an item. Returns false on timeout or shutdown+empty.
    bool popFor(T& value, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!condition_.wait_for(lock, timeout, [this] { return shutdown_ || !queue_.empty(); })) {
            return false;
        }
        if (queue_.empty()) {
            return false;
        }
        value = std::move(queue_.front());
        queue_.pop();
        return true;
    }

    // Non-blocking pop, used to drain a batch quickly.
    bool tryPop(T& value) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) {
            return false;
        }
        value = std::move(queue_.front());
        queue_.pop();
        return true;
    }

    void shutdown() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            shutdown_ = true;
        }
        condition_.notify_all();
    }

    std::size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

private:
    std::queue<T> queue_;
    mutable std::mutex mutex_;
    std::condition_variable condition_;
    bool shutdown_ = false;
};

// ----------------------------------------------------------------------------
// Engine statistics. All counters share one mutex; contention is negligible
// since increments are O(1) and calls are made outside the OrderBook's own
// critical section wherever possible.
// ----------------------------------------------------------------------------

class OrderBookStats {
public:
    void onOrderReceived() { std::lock_guard<std::mutex> lock(mutex_); ++ordersReceived_; }
    void onOrderAccepted() { std::lock_guard<std::mutex> lock(mutex_); ++ordersAccepted_; }
    void onOrderRejected() { std::lock_guard<std::mutex> lock(mutex_); ++ordersRejected_; }
    void onOrderMatched()  { std::lock_guard<std::mutex> lock(mutex_); ++ordersMatched_; }
    void onOrderModified() { std::lock_guard<std::mutex> lock(mutex_); ++ordersModified_; }
    void onOrderCancelled(){ std::lock_guard<std::mutex> lock(mutex_); ++ordersCancelled_; }
    void onTradeExecuted() { std::lock_guard<std::mutex> lock(mutex_); ++tradesExecuted_; }

    void onMatchingLatency(std::chrono::microseconds latency) {
        std::lock_guard<std::mutex> lock(mutex_);
        ++matchingCalls_;
        const auto latencyUs = static_cast<std::uint64_t>(latency.count());
        totalMatchingLatencyUs_ += latencyUs;
        maxMatchingLatencyUs_ = std::max(maxMatchingLatencyUs_, latencyUs);
    }

    void setQueueSize(std::size_t size) { std::lock_guard<std::mutex> lock(mutex_); currentQueueSize_ = size; }
    void setActiveOrders(std::size_t activeOrders) { std::lock_guard<std::mutex> lock(mutex_); activeOrders_ = activeOrders; }

    struct Snapshot {
        std::uint64_t ordersReceived = 0;
        std::uint64_t ordersAccepted = 0;
        std::uint64_t ordersRejected = 0;
        std::uint64_t ordersMatched = 0;
        std::uint64_t ordersModified = 0;
        std::uint64_t ordersCancelled = 0;
        std::uint64_t tradesExecuted = 0;
        std::size_t queueSize = 0;
        std::size_t activeOrders = 0;
        double averageMatchingLatencyMs = 0.0;
        double maxMatchingLatencyMs = 0.0;
        double ordersPerSecond = 0.0;
        double tradesPerSecond = 0.0;
        double cancelRate = 0.0; // cancelled / accepted
        double fillRate = 0.0;   // matched / accepted
    };

    Snapshot snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        Snapshot snap;
        snap.ordersReceived = ordersReceived_;
        snap.ordersAccepted = ordersAccepted_;
        snap.ordersRejected = ordersRejected_;
        snap.ordersMatched = ordersMatched_;
        snap.ordersModified = ordersModified_;
        snap.ordersCancelled = ordersCancelled_;
        snap.tradesExecuted = tradesExecuted_;
        snap.queueSize = currentQueueSize_;
        snap.activeOrders = activeOrders_;
        if (matchingCalls_ > 0) {
            snap.averageMatchingLatencyMs = static_cast<double>(totalMatchingLatencyUs_) / static_cast<double>(matchingCalls_) / 1000.0;
            snap.maxMatchingLatencyMs = static_cast<double>(maxMatchingLatencyUs_) / 1000.0;
        }
        const auto elapsed = std::chrono::steady_clock::now() - startTime_;
        const double seconds = std::max(1e-3, std::chrono::duration<double>(elapsed).count());
        snap.ordersPerSecond = static_cast<double>(ordersReceived_) / seconds;
        snap.tradesPerSecond = static_cast<double>(tradesExecuted_) / seconds;
        if (ordersAccepted_ > 0) {
            snap.cancelRate = static_cast<double>(ordersCancelled_) / static_cast<double>(ordersAccepted_);
            snap.fillRate = static_cast<double>(ordersMatched_) / static_cast<double>(ordersAccepted_);
        }
        return snap;
    }

private:
    mutable std::mutex mutex_;
    std::chrono::steady_clock::time_point startTime_ = std::chrono::steady_clock::now();
    std::uint64_t ordersReceived_ = 0;
    std::uint64_t ordersAccepted_ = 0;
    std::uint64_t ordersRejected_ = 0;
    std::uint64_t ordersMatched_ = 0;
    std::uint64_t ordersModified_ = 0;
    std::uint64_t ordersCancelled_ = 0;
    std::uint64_t tradesExecuted_ = 0;
    std::uint64_t matchingCalls_ = 0;
    std::uint64_t totalMatchingLatencyUs_ = 0;
    std::uint64_t maxMatchingLatencyUs_ = 0;
    std::size_t currentQueueSize_ = 0;
    std::size_t activeOrders_ = 0;
};

// ----------------------------------------------------------------------------
// Persistence layer.
//
// Design choices vs. the original implementation:
//   - Events are batched (bounded by count AND time) and written inside a
//     single BEGIN/COMMIT instead of one transaction per row, which is both
//     far cheaper for Postgres and guarantees the matching thread is never
//     slowed down by per-row round trips.
//   - Order upserts use INSERT ... ON CONFLICT DO UPDATE because an order is
//     persisted multiple times over its life (new -> modify -> cancel/fill).
//     The original schema used a bare INSERT with order_id as PRIMARY KEY,
//     which would violate the primary key and fail on every modify/cancel.
//   - A failed batch is retried (with backoff) rather than discarded, so a
//     transient DB outage cannot silently lose order/trade history.
//   - All PGresult/PGconn objects are owned by RAII wrappers; no manual
//     PQclear/PQfinish call sites remain.
// ----------------------------------------------------------------------------

struct DbEvent {
    enum class Type { InsertOrder, InsertTrade } type;
    Order order;
    TradeRecord trade;
};

class AsyncPersistenceStore {
public:
    explicit AsyncPersistenceStore(ThreadSafeLogger& logger) : logger_(logger) {}

    ~AsyncPersistenceStore() {
        stop();
        disconnect();
    }

    AsyncPersistenceStore(const AsyncPersistenceStore&) = delete;
    AsyncPersistenceStore& operator=(const AsyncPersistenceStore&) = delete;

    bool connect(const std::string& connInfo = "") {
        std::lock_guard<std::mutex> lock(connMutex_);
        const std::string effective = connInfo.empty() ? buildConnectionString() : connInfo;
        PGConnPtr conn(PQconnectdb(effective.c_str()));
        if (!conn) {
            logger_.logError("postgres", "PQconnectdb returned a null connection handle");
            return false;
        }
        if (PQstatus(conn.get()) != CONNECTION_OK) {
            logger_.logError("postgres", std::string("connection failed: ") + PQerrorMessage(conn.get()));
            return false;
        }
        conn_ = std::move(conn);
        if (!ensureSchema() || !prepareStatements()) {
            conn_.reset();
            return false;
        }
        connected_ = true;
        return true;
    }

    void start() {
        running_.store(true, std::memory_order_release);
        worker_ = std::thread([this] { workerLoop(); });
    }

    void stop() {
        running_.store(false, std::memory_order_release);
        queue_.shutdown();
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    void enqueueOrder(const Order& order) { queue_.push(DbEvent{DbEvent::Type::InsertOrder, order, {}}); }
    void enqueueTrade(const TradeRecord& trade) { queue_.push(DbEvent{DbEvent::Type::InsertTrade, {}, trade}); }

    bool connected() const {
        std::lock_guard<std::mutex> lock(connMutex_);
        return connected_ && conn_ && PQstatus(conn_.get()) == CONNECTION_OK;
    }

    void disconnect() {
        std::lock_guard<std::mutex> lock(connMutex_);
        conn_.reset();
        connected_ = false;
    }

private:
    static constexpr std::size_t kBatchSize = 100;
    static constexpr auto kBatchWindow = 100ms;
    static constexpr auto kRetryBackoff = 250ms;

    void workerLoop() {
        std::vector<DbEvent> batch;
        batch.reserve(kBatchSize);
        while (running_.load(std::memory_order_acquire) || !batch.empty()) {
            DbEvent event;
            // Wait for the first event of a batch (or drain time-boxed).
            if (batch.empty()) {
                if (!queue_.popFor(event, kBatchWindow)) {
                    if (!running_.load(std::memory_order_acquire)) {
                        break; // nothing left, shutting down
                    }
                    continue;
                }
                batch.push_back(std::move(event));
            }
            // Opportunistically drain more without blocking, up to batch size.
            while (batch.size() < kBatchSize && queue_.tryPop(event)) {
                batch.push_back(std::move(event));
            }

            if (!connected()) {
                logger_.logReconnectAttempt("attempting database reconnect");
                if (!connect()) {
                    std::this_thread::sleep_for(kRetryBackoff);
                    continue; // batch retained, retried next iteration - no data loss
                }
            }

            if (flushBatch(batch)) {
                batch.clear();
            } else {
                logger_.logWarning("postgres", "batch persistence failed; will retry entire batch");
                std::this_thread::sleep_for(kRetryBackoff);
            }
        }
    }

    bool flushBatch(const std::vector<DbEvent>& batch) {
        std::lock_guard<std::mutex> lock(connMutex_);
        if (!conn_ || PQstatus(conn_.get()) != CONNECTION_OK) {
            connected_ = false;
            return false;
        }
        PGResultPtr begin(PQexec(conn_.get(), "BEGIN"));
        if (!begin || PQresultStatus(begin.get()) != PGRES_COMMAND_OK) {
            return false;
        }
        for (const auto& event : batch) {
            const bool ok = (event.type == DbEvent::Type::InsertOrder) ? persistOrderLocked(event.order)
                                                                        : persistTradeLocked(event.trade);
            if (!ok) {
                PGResultPtr rollback(PQexec(conn_.get(), "ROLLBACK"));
                return false;
            }
        }
        PGResultPtr commit(PQexec(conn_.get(), "COMMIT"));
        if (!commit || PQresultStatus(commit.get()) != PGRES_COMMAND_OK) {
            logger_.logError("postgres", std::string("batch commit failed: ") + (commit ? PQresultErrorMessage(commit.get()) : "null result"));
            PGResultPtr rollback(PQexec(conn_.get(), "ROLLBACK"));
            return false;
        }
        logger_.logPostgres("batch_commit", "rows=" + std::to_string(batch.size()));
        return true;
    }

    // Caller must hold connMutex_.
    bool persistOrderLocked(const Order& order) {
        const std::string orderIdText = std::to_string(order.id);
        const std::string priceText = std::to_string(order.price);
        const std::string quantityText = std::to_string(order.quantity);
        const char* side = toString(order.side);
        const char* type = toString(order.type);
        const char* tif = toString(order.tif);
        const char* status = toString(order.status);

        const char* values[] = {
            orderIdText.c_str(), order.clientId.c_str(), order.clientOrderId.c_str(), order.symbol.c_str(),
            side, type, tif, priceText.c_str(), quantityText.c_str(), status
        };
        const int lengths[] = {
            static_cast<int>(orderIdText.size()), static_cast<int>(order.clientId.size()),
            static_cast<int>(order.clientOrderId.size()), static_cast<int>(order.symbol.size()),
            static_cast<int>(std::strlen(side)), static_cast<int>(std::strlen(type)),
            static_cast<int>(std::strlen(tif)), static_cast<int>(priceText.size()),
            static_cast<int>(quantityText.size()), static_cast<int>(std::strlen(status))
        };
        const int formats[10] = {0,0,0,0,0,0,0,0,0,0};

        PGResultPtr result(PQexecPrepared(conn_.get(), "upsert_order_stmt", 10, values, lengths, formats, 0));
        if (!result || PQresultStatus(result.get()) != PGRES_COMMAND_OK) {
            logger_.logError("postgres", std::string("order upsert failed: ") + (result ? PQresultErrorMessage(result.get()) : "null result"));
            return false;
        }
        return true;
    }

    // Caller must hold connMutex_.
    bool persistTradeLocked(const TradeRecord& trade) {
        const std::string tradeIdText = std::to_string(trade.tradeId);
        const std::string buyOrderText = std::to_string(trade.buyOrderId);
        const std::string sellOrderText = std::to_string(trade.sellOrderId);
        const std::string priceText = std::to_string(trade.price);
        const std::string quantityText = std::to_string(trade.quantity);

        const char* values[] = {
            tradeIdText.c_str(), buyOrderText.c_str(), sellOrderText.c_str(),
            trade.symbol.c_str(), priceText.c_str(), quantityText.c_str()
        };
        const int lengths[] = {
            static_cast<int>(tradeIdText.size()), static_cast<int>(buyOrderText.size()),
            static_cast<int>(sellOrderText.size()), static_cast<int>(trade.symbol.size()),
            static_cast<int>(priceText.size()), static_cast<int>(quantityText.size())
        };
        const int formats[6] = {0,0,0,0,0,0};

        PGResultPtr result(PQexecPrepared(conn_.get(), "insert_trade_stmt", 6, values, lengths, formats, 0));
        if (!result || PQresultStatus(result.get()) != PGRES_COMMAND_OK) {
            logger_.logError("postgres", std::string("trade insert failed: ") + (result ? PQresultErrorMessage(result.get()) : "null result"));
            return false;
        }
        return true;
    }

    // Caller must hold connMutex_.
    bool ensureSchema() {
        static const char* statements[] = {
            "CREATE TABLE IF NOT EXISTS order_history ("
            " order_id BIGINT PRIMARY KEY,"
            " client_id TEXT NOT NULL,"
            " client_order_id TEXT NOT NULL,"
            " symbol TEXT NOT NULL,"
            " side TEXT NOT NULL,"
            " order_type TEXT NOT NULL,"
            " time_in_force TEXT NOT NULL,"
            " price DOUBLE PRECISION NOT NULL,"
            " quantity INTEGER NOT NULL,"
            " status TEXT NOT NULL,"
            " created_at TIMESTAMPTZ NOT NULL DEFAULT now(),"
            " updated_at TIMESTAMPTZ NOT NULL DEFAULT now())",

            "CREATE TABLE IF NOT EXISTS trade_history ("
            " trade_id BIGINT PRIMARY KEY,"
            " buy_order_id BIGINT NOT NULL,"
            " sell_order_id BIGINT NOT NULL,"
            " symbol TEXT NOT NULL,"
            " execution_price DOUBLE PRECISION NOT NULL,"
            " quantity INTEGER NOT NULL,"
            " executed_at TIMESTAMPTZ NOT NULL DEFAULT now())",

            "CREATE INDEX IF NOT EXISTS idx_order_history_symbol ON order_history(symbol)",
            "CREATE INDEX IF NOT EXISTS idx_order_history_status ON order_history(status)",
            "CREATE INDEX IF NOT EXISTS idx_order_history_client ON order_history(client_id)",
            "CREATE INDEX IF NOT EXISTS idx_trade_history_symbol ON trade_history(symbol)",
            "CREATE INDEX IF NOT EXISTS idx_trade_history_time ON trade_history(executed_at)"
        };
        for (const char* statement : statements) {
            PGResultPtr result(PQexec(conn_.get(), statement));
            if (!result || PQresultStatus(result.get()) != PGRES_COMMAND_OK) {
                logger_.logError("postgres", std::string("schema create failed: ") + (result ? PQresultErrorMessage(result.get()) : "null result"));
                return false;
            }
        }
        return true;
    }

    // Caller must hold connMutex_.
    bool prepareStatements() {
        // ON CONFLICT DO UPDATE: an order row is written on NEW, MODIFY, CANCEL
        // and FILL; only the first write is a true insert, the rest must update
        // the existing row rather than violate the primary key.
        const char* orderStatement =
            "INSERT INTO order_history (order_id, client_id, client_order_id, symbol, side, order_type, time_in_force, price, quantity, status, created_at, updated_at) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, now(), now()) "
            "ON CONFLICT (order_id) DO UPDATE SET "
            "price = EXCLUDED.price, quantity = EXCLUDED.quantity, status = EXCLUDED.status, updated_at = now()";
        const char* tradeStatement =
            "INSERT INTO trade_history (trade_id, buy_order_id, sell_order_id, symbol, execution_price, quantity, executed_at) "
            "VALUES ($1,$2,$3,$4,$5,$6, now()) "
            "ON CONFLICT (trade_id) DO NOTHING";

        PGResultPtr orderPrep(PQprepare(conn_.get(), "upsert_order_stmt", orderStatement, 10, nullptr));
        if (!orderPrep || (PQresultStatus(orderPrep.get()) != PGRES_COMMAND_OK && PQresultStatus(orderPrep.get()) != PGRES_TUPLES_OK)) {
            logger_.logError("postgres", std::string("prepare order upsert failed: ") + (orderPrep ? PQresultErrorMessage(orderPrep.get()) : "null result"));
            return false;
        }
        PGResultPtr tradePrep(PQprepare(conn_.get(), "insert_trade_stmt", tradeStatement, 7 - 1, nullptr));
        if (!tradePrep || (PQresultStatus(tradePrep.get()) != PGRES_COMMAND_OK && PQresultStatus(tradePrep.get()) != PGRES_TUPLES_OK)) {
            logger_.logError("postgres", std::string("prepare trade insert failed: ") + (tradePrep ? PQresultErrorMessage(tradePrep.get()) : "null result"));
            return false;
        }
        return true;
    }

    std::string buildConnectionString() const {
        const char* host = std::getenv("PGHOST");
        const char* port = std::getenv("PGPORT");
        const char* database = std::getenv("PGDATABASE");
        const char* user = std::getenv("PGUSER");
        const char* password = std::getenv("PGPASSWORD");
        std::ostringstream stream;
        stream << "host=" << (host ? host : "127.0.0.1")
               << " port=" << (port ? port : "5432")
               << " dbname=" << (database ? database : "postgres")
               << " user=" << (user ? user : "postgres")
               << " connect_timeout=5";
        if (password != nullptr && password[0] != '\0') {
            stream << " password=" << password;
        }
        return stream.str();
    }

    ThreadSafeLogger& logger_;
    mutable std::mutex connMutex_;
    ThreadSafeQueue<DbEvent> queue_;
    std::thread worker_;
    std::atomic<bool> running_{false};
    bool connected_ = false;
    PGConnPtr conn_;
};

// ----------------------------------------------------------------------------
// OrderBook: the matching engine core.
//
// Concurrency model: a single mutex_ guards the book's internal state
// (orders_, buyLevels_, sellLevels_, clientOrderKeys_, tradeHistory_,
// counters). The critical section is kept as small as possible: matching
// logic runs under the lock, but logging and persistence enqueueing happen
// AFTER the lock is released (events are buffered locally then flushed),
// so slow I/O never serializes concurrent order submissions.
// ----------------------------------------------------------------------------

class OrderBook {
public:
    OrderBook(ThreadSafeLogger& logger, OrderBookStats& stats, AsyncPersistenceStore& persistence,
              SelfTradePolicy stpPolicy = SelfTradePolicy::CancelResting)
        : logger_(logger), stats_(stats), persistence_(persistence), stpPolicy_(stpPolicy) {}

    // Parses and processes one wire message. Supports:
    //   New order (backward compatible with the original 5/6-field format,
    //     extended with optional leading TIF and trailing clientId fields):
    //       symbol|side|price|qty|type
    //       symbol|side|price|qty|type|clientOrderId
    //       tif|symbol|side|price|qty|type|clientOrderId
    //       tif|symbol|side|price|qty|type|clientOrderId|clientId
    //   Cancel:  CANCEL|orderId|clientId
    //   Modify:  MODIFY|orderId|newPrice|newQty|clientId
    OrderExecutionResult processMessage(const std::string& rawMessage, const std::string& clientIp) {
        stats_.onOrderReceived();
        std::vector<std::string> parts = split(rawMessage, '|');
        if (parts.empty()) {
            logger_.logValidationFailure("empty message", clientIp);
            stats_.onOrderRejected();
            return {false, 0, "ERROR|VALIDATION|empty message"};
        }

        if (parts[0] == "CANCEL") {
            return handleCancelMessage(parts, clientIp);
        }
        if (parts[0] == "MODIFY") {
            return handleModifyMessage(parts, clientIp);
        }
        return handleNewOrderMessage(parts, clientIp);
    }

    // Authorized cancel: only the owning clientId may cancel its own order.
    bool cancelOrder(std::uint64_t orderId, const std::string& clientId) {
        Order snapshot;
        bool cancelled = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            Order* existing = findOrderLocked(orderId);
            if (!existing || !existing->active) {
                logger_.logWarning("orderbook", "cancel_failed order not found or inactive order_id=" + std::to_string(orderId));
                return false;
            }
            if (!clientId.empty() && existing->clientId != clientId) {
                logger_.logWarning("orderbook", "cancel_rejected ownership mismatch order_id=" + std::to_string(orderId));
                return false;
            }
            existing->active = false;
            existing->status = OrderStatus::Cancelled;
            removeFromBookLocked(existing);
            clientOrderKeys_.erase(clientOrderKey(existing->clientId, existing->clientOrderId));
            stats_.setActiveOrders(orders_.size());
            snapshot = *existing;
            cancelled = true;
        }
        // Logging & persistence happen outside the lock.
        if (cancelled) {
            stats_.onOrderCancelled();
            persistence_.enqueueOrder(snapshot);
            logger_.logOrderCancelled(snapshot);
        }
        return cancelled;
    }

    bool modifyOrder(std::uint64_t orderId, double newPrice, int newQuantity, const std::string& clientId) {
        Order snapshot;
        std::vector<TradeRecord> newTrades;
        std::vector<Order> touchedOrders;
        bool modified = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            Order* existing = findOrderLocked(orderId);
            if (!existing || !existing->active) {
                logger_.logWarning("orderbook", "modify_failed order not found or inactive order_id=" + std::to_string(orderId));
                return false;
            }
            if (!clientId.empty() && existing->clientId != clientId) {
                logger_.logWarning("orderbook", "modify_rejected ownership mismatch order_id=" + std::to_string(orderId));
                return false;
            }
            if (newQuantity <= 0 || (existing->type == OrderType::Market ? newPrice < 0.0 : newPrice <= 0.0)) {
                logger_.logWarning("orderbook", "modify_rejected invalid price/quantity order_id=" + std::to_string(orderId));
                return false;
            }

            const int oldQuantity = existing->quantity;
            removeFromBookLocked(existing);
            existing->price = newPrice;
            existing->quantity = newQuantity;
            existing->originalQuantity = newQuantity;
            existing->sequence = ++prioritySequence_; // any modification loses time priority (industry standard)
            // Correct status transition logic: compare against the PRE-modification
            // quantity, not the value we just overwrote (the original code compared
            // quantity to itself, which is always false and produced wrong statuses).
            existing->status = (newQuantity < oldQuantity) ? OrderStatus::PartiallyFilled : OrderStatus::New;
            addToBookLocked(*existing);
            matchOrdersLocked(existing, newTrades, touchedOrders);

            stats_.onOrderModified();
            stats_.setActiveOrders(orders_.size());
            snapshot = *existing;
            modified = true;
        }
        if (modified) {
            persistence_.enqueueOrder(snapshot);
            logger_.logOrderModified(snapshot);
            publishTrades(newTrades, touchedOrders);
        }
        return modified;
    }

    std::vector<std::string> getMarketDepth() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<std::string> depth;
        depth.reserve(buyLevels_.size() + sellLevels_.size());
        for (auto it = buyLevels_.rbegin(); it != buyLevels_.rend(); ++it) {
            long long totalQty = 0;
            for (const auto* order : it->second) {
                totalQty += order->quantity;
            }
            depth.emplace_back("BUY " + std::to_string(it->first) + " x " + std::to_string(totalQty));
        }
        for (const auto& [price, level] : sellLevels_) {
            long long totalQty = 0;
            for (const auto* order : level) {
                totalQty += order->quantity;
            }
            depth.emplace_back("SELL " + std::to_string(price) + " x " + std::to_string(totalQty));
        }
        return depth;
    }

    std::pair<double, double> getBestBidAsk() const {
        std::lock_guard<std::mutex> lock(mutex_);
        double bestBid = 0.0;
        double bestAsk = 0.0;
        if (!buyLevels_.empty()) bestBid = buyLevels_.rbegin()->first;
        if (!sellLevels_.empty()) bestAsk = sellLevels_.begin()->first;
        return {bestBid, bestAsk};
    }

    std::vector<TradeRecord> getTradeHistory() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return tradeHistory_;
    }

    std::size_t activeOrderCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return activeCount_;
    }

private:
    // ------------------------------------------------------------------
    // Message parsing / dispatch
    // ------------------------------------------------------------------

    OrderExecutionResult handleCancelMessage(const std::vector<std::string>& parts, const std::string& clientIp) {
        if (parts.size() < 2) {
            logger_.logValidationFailure("malformed cancel message", clientIp);
            stats_.onOrderRejected();
            return {false, 0, "ERROR|VALIDATION|malformed cancel message"};
        }
        std::uint64_t orderId = 0;
        try {
            orderId = std::stoull(parts[1]);
        } catch (...) {
            logger_.logValidationFailure("invalid order id in cancel", clientIp);
            stats_.onOrderRejected();
            return {false, 0, "ERROR|VALIDATION|invalid order id"};
        }
        const std::string clientId = parts.size() >= 3 ? parts[2] : clientIp;
        const bool ok = cancelOrder(orderId, clientId);
        if (!ok) {
            return {false, orderId, "ERROR|CANCEL|order not found, inactive, or not owned by caller"};
        }
        return {true, orderId, "ACK|CANCEL|" + std::to_string(orderId)};
    }

    OrderExecutionResult handleModifyMessage(const std::vector<std::string>& parts, const std::string& clientIp) {
        if (parts.size() < 4) {
            logger_.logValidationFailure("malformed modify message", clientIp);
            stats_.onOrderRejected();
            return {false, 0, "ERROR|VALIDATION|malformed modify message"};
        }
        std::uint64_t orderId = 0;
        double newPrice = 0.0;
        int newQty = 0;
        try {
            orderId = std::stoull(parts[1]);
            newPrice = std::stod(parts[2]);
            newQty = std::stoi(parts[3]);
        } catch (...) {
            logger_.logValidationFailure("invalid numeric field in modify", clientIp);
            stats_.onOrderRejected();
            return {false, 0, "ERROR|VALIDATION|invalid modify fields"};
        }
        const std::string clientId = parts.size() >= 5 ? parts[4] : clientIp;
        const bool ok = modifyOrder(orderId, newPrice, newQty, clientId);
        if (!ok) {
            return {false, orderId, "ERROR|MODIFY|rejected (not found, not owned, or invalid values)"};
        }
        return {true, orderId, "ACK|MODIFY|" + std::to_string(orderId)};
    }

    OrderExecutionResult handleNewOrderMessage(const std::vector<std::string>& parts, const std::string& clientIp) {
        std::string tifText = "GTC";
        std::string symbol, side, priceText, qtyText, typeText, clientOrderId, clientId;

        if (parts.size() == 5) {
            symbol = parts[0]; side = parts[1]; priceText = parts[2]; qtyText = parts[3]; typeText = parts[4];
            clientOrderId = "auto-" + std::to_string(nextAutoClientOrderId());
            clientId = clientIp;
        } else if (parts.size() == 6) {
            symbol = parts[0]; side = parts[1]; priceText = parts[2]; qtyText = parts[3]; typeText = parts[4];
            clientOrderId = parts[5];
            clientId = clientIp;
        } else if (parts.size() == 7) {
            tifText = parts[0]; symbol = parts[1]; side = parts[2]; priceText = parts[3]; qtyText = parts[4]; typeText = parts[5];
            clientOrderId = parts[6];
            clientId = clientIp;
        } else if (parts.size() == 8) {
            tifText = parts[0]; symbol = parts[1]; side = parts[2]; priceText = parts[3]; qtyText = parts[4]; typeText = parts[5];
            clientOrderId = parts[6];
            clientId = parts[7];
        } else {
            logger_.logValidationFailure("malformed message", clientIp);
            stats_.onOrderRejected();
            return {false, 0, "ERROR|VALIDATION|malformed message"};
        }

        const ValidationResult validation = validateRequest(symbol, side, priceText, qtyText, typeText, tifText, clientOrderId);
        if (!validation.ok) {
            logger_.logValidationFailure(validation.error, clientIp);
            stats_.onOrderRejected();
            return {false, 0, "ERROR|VALIDATION|" + validation.error};
        }

        const bool isMarket = (typeText == "MARKET");
        const bool isBuy = (side == "BUY");
        const double price = isMarket ? 0.0 : std::stod(priceText);
        const int quantity = std::stoi(qtyText);
        const TimeInForce tif = parseTif(tifText);

        Order newOrder;
        newOrder.clientOrderId = clientOrderId;
        newOrder.clientId = clientId;
        newOrder.clientIp = clientIp;
        newOrder.symbol = symbol;
        newOrder.side = isBuy ? Side::Buy : Side::Sell;
        newOrder.type = isMarket ? OrderType::Market : OrderType::Limit;
        newOrder.tif = tif;
        newOrder.price = price;
        newOrder.quantity = quantity;
        newOrder.originalQuantity = quantity;
        newOrder.active = true;
        newOrder.status = OrderStatus::New;

        std::vector<TradeRecord> newTrades;
        std::vector<Order> touchedOrders;
        Order snapshot;
        std::uint64_t assignedId = 0;

        {
            std::lock_guard<std::mutex> lock(mutex_);
            const std::string key = clientOrderKey(newOrder.clientId, newOrder.clientOrderId);
            if (clientOrderKeys_.find(key) != clientOrderKeys_.end()) {
                logger_.logValidationFailure("duplicate client order id", clientIp);
                stats_.onOrderRejected();
                return {false, 0, "ERROR|VALIDATION|duplicate client order id"};
            }

            // Fill-Or-Kill must be checked BEFORE any state mutation: if the
            // resting liquidity (excluding self-trades, which will not execute)
            // cannot satisfy the full quantity, the order is rejected outright
            // with zero market impact.
            if (tif == TimeInForce::FOK && !canFullyFillLocked(newOrder)) {
                logger_.logValidationFailure("FOK cannot be fully filled", clientIp);
                stats_.onOrderRejected();
                return {false, 0, "ERROR|FOK|insufficient liquidity, order killed"};
            }

            assignedId = ++idCounter_;
            newOrder.id = assignedId;
            newOrder.sequence = ++prioritySequence_;

            auto [it, inserted] = orders_.emplace(assignedId, std::move(newOrder));
            (void)inserted;
            Order* stored = &it->second;
            clientOrderKeys_.insert(key);
            ++activeCount_;

            if (stored->type == OrderType::Market) {
                executeMarketOrderLocked(*stored, newTrades, touchedOrders);
            } else {
                addToBookLocked(*stored);
                matchOrdersLocked(stored, newTrades, touchedOrders);
            }

            // IOC (and market orders behave the same way): whatever quantity
            // could not be matched immediately must not rest in the book.
            if (stored->active && stored->quantity > 0 && stored->tif == TimeInForce::IOC) {
                removeFromBookLocked(stored);
                stored->active = false;
                stored->status = OrderStatus::Cancelled;
                clientOrderKeys_.erase(clientOrderKey(stored->clientId, stored->clientOrderId));
            }

            if (!stored->active) {
                --activeCount_;
            }

            stats_.setActiveOrders(activeCount_);
            snapshot = *stored;
        }

        stats_.onOrderAccepted();
        if (snapshot.status == OrderStatus::Filled || snapshot.status == OrderStatus::Cancelled) {
            if (snapshot.status == OrderStatus::Cancelled) {
                stats_.onOrderCancelled();
            }
        }
        persistence_.enqueueOrder(snapshot);
        logger_.logOrderAdded(snapshot);
        publishTrades(newTrades, touchedOrders);

        return {true, snapshot.id, "ACK|" + std::to_string(snapshot.id) + "|" + toString(snapshot.status) + "|" + std::to_string(snapshot.quantity)};
    }

    // Emits trade + order-touched logging/persistence for events collected
    // while the lock was held. Always called AFTER mutex_ has been released.
    void publishTrades(const std::vector<TradeRecord>& trades, const std::vector<Order>& touchedOrders) {
        for (const auto& order : touchedOrders) {
            persistence_.enqueueOrder(order);
        }
        for (const auto& trade : trades) {
            stats_.onTradeExecuted();
            persistence_.enqueueTrade(trade);
            logger_.logTradeExecuted(trade);
        }
    }

    static TimeInForce parseTif(const std::string& text) {
        if (text == "IOC") return TimeInForce::IOC;
        if (text == "FOK") return TimeInForce::FOK;
        return TimeInForce::GTC;
    }

    std::uint64_t nextAutoClientOrderId() {
        return clientSequenceCounter_.fetch_add(1, std::memory_order_relaxed);
    }

    // ------------------------------------------------------------------
    // Validation
    // ------------------------------------------------------------------

    static constexpr std::size_t kMaxSymbolLength = 16;
    static constexpr std::size_t kMaxClientOrderIdLength = 64;
    static constexpr int kMaxQuantity = 1'000'000'000;
    static constexpr double kMaxPrice = 1'000'000'000.0;

    ValidationResult validateRequest(const std::string& symbol, const std::string& side, const std::string& priceText,
                                      const std::string& qtyText, const std::string& typeText, const std::string& tifText,
                                      const std::string& clientOrderId) const {
        if (symbol.empty() || symbol.size() > kMaxSymbolLength || !isValidSymbol(symbol)) {
            return {false, "invalid symbol"};
        }
        if (side != "BUY" && side != "SELL") {
            return {false, "invalid side"};
        }
        if (clientOrderId.empty() || clientOrderId.size() > kMaxClientOrderIdLength) {
            return {false, "missing or oversized client order id"};
        }
        if (tifText != "GTC" && tifText != "IOC" && tifText != "FOK") {
            return {false, "invalid time in force"};
        }
        if (qtyText.empty()) {
            return {false, "missing quantity"};
        }
        int quantity = 0;
        try {
            quantity = std::stoi(qtyText);
        } catch (...) {
            return {false, "invalid or out-of-range quantity"};
        }
        if (quantity <= 0 || quantity > kMaxQuantity) {
            return {false, "quantity out of range"};
        }
        if (typeText != "MARKET" && typeText != "LIMIT") {
            return {false, "invalid order type"};
        }
        const bool isMarket = (typeText == "MARKET");
        if (!isMarket) {
            if (priceText.empty()) {
                return {false, "missing price"};
            }
            double price = 0.0;
            try {
                price = std::stod(priceText);
            } catch (...) {
                return {false, "invalid or out-of-range price"};
            }
            if (price <= 0.0 || price > kMaxPrice) {
                return {false, "limit price out of range"};
            }
        } else if (!priceText.empty()) {
            double price = 0.0;
            try {
                price = std::stod(priceText);
            } catch (...) {
                return {false, "invalid market price field"};
            }
            if (price < 0.0) {
                return {false, "market price cannot be negative"};
            }
        }
        return {true, ""};
    }

    static bool isValidSymbol(const std::string& symbol) {
        for (unsigned char ch : symbol) {
            if (!std::isalnum(ch)) {
                return false;
            }
        }
        return true;
    }

    static std::vector<std::string> split(const std::string& message, char delimiter) {
        std::vector<std::string> parts;
        std::stringstream stream(message);
        std::string token;
        while (std::getline(stream, token, delimiter)) {
            parts.push_back(token);
        }
        return parts;
    }

    static std::string clientOrderKey(const std::string& clientId, const std::string& clientOrderId) {
        std::string key;
        key.reserve(clientId.size() + clientOrderId.size() + 1);
        key += clientId;
        key += '\x1f';
        key += clientOrderId;
        return key;
    }

    // ------------------------------------------------------------------
    // Matching engine internals (all require mutex_ to be held by the caller)
    // ------------------------------------------------------------------

    Order* findOrderLocked(std::uint64_t orderId) {
        auto it = orders_.find(orderId);
        return it == orders_.end() ? nullptr : &it->second;
    }

    Order* getBestBuyOrderLocked() {
        for (auto it = buyLevels_.rbegin(); it != buyLevels_.rend(); ++it) {
            if (!it->second.empty()) {
                return it->second.front();
            }
        }
        return nullptr;
    }

    Order* getBestSellOrderLocked() {
        for (auto& [price, level] : sellLevels_) {
            if (!level.empty()) {
                return level.front();
            }
        }
        return nullptr;
    }

    void addToBookLocked(Order& order) {
        if (order.side == Side::Buy) {
            buyLevels_[order.price].push_back(&order);
        } else {
            sellLevels_[order.price].push_back(&order);
        }
    }

    void removeFromBookLocked(Order* order) {
        // buyLevels_ and sellLevels_ use different comparators (descending vs
        // ascending), so they are distinct types and cannot share a single
        // reference; a small helper lambda avoids duplicating the erase logic.
        auto eraseFrom = [order](auto& levels) {
            auto it = levels.find(order->price);
            if (it == levels.end()) {
                return;
            }
            auto& level = it->second;
            auto pos = std::find(level.begin(), level.end(), order);
            if (pos != level.end()) {
                level.erase(pos);
            }
            if (level.empty()) {
                levels.erase(it);
            }
        };
        if (order->side == Side::Buy) {
            eraseFrom(buyLevels_);
        } else {
            eraseFrom(sellLevels_);
        }
    }

    // Returns true if a self-trade prevention action was taken. `aggressor`
    // is the order that initiated this matching pass (the newly-added or
    // just-modified order); `resting` is its prospective counterparty.
    bool applySelfTradePreventionLocked(Order& aggressor, Order* resting, std::vector<Order>& touchedOrders) {
        if (aggressor.clientId.empty() || resting->clientId.empty() || aggressor.clientId != resting->clientId) {
            return false;
        }
        switch (stpPolicy_) {
            case SelfTradePolicy::CancelResting: {
                resting->active = false;
                resting->status = OrderStatus::Cancelled;
                removeFromBookLocked(resting);
                clientOrderKeys_.erase(clientOrderKey(resting->clientId, resting->clientOrderId));
                --activeCount_;
                stats_.onOrderCancelled();
                touchedOrders.push_back(*resting);
                break;
            }
            case SelfTradePolicy::CancelAggressor: {
                aggressor.active = false;
                aggressor.status = OrderStatus::Cancelled;
                removeFromBookLocked(&aggressor);
                clientOrderKeys_.erase(clientOrderKey(aggressor.clientId, aggressor.clientOrderId));
                --activeCount_;
                stats_.onOrderCancelled();
                touchedOrders.push_back(aggressor);
                break;
            }
            case SelfTradePolicy::CancelBoth: {
                resting->active = false;
                resting->status = OrderStatus::Cancelled;
                removeFromBookLocked(resting);
                clientOrderKeys_.erase(clientOrderKey(resting->clientId, resting->clientOrderId));
                --activeCount_;
                aggressor.active = false;
                aggressor.status = OrderStatus::Cancelled;
                removeFromBookLocked(&aggressor);
                clientOrderKeys_.erase(clientOrderKey(aggressor.clientId, aggressor.clientOrderId));
                --activeCount_;
                stats_.onOrderCancelled();
                stats_.onOrderCancelled();
                touchedOrders.push_back(*resting);
                touchedOrders.push_back(aggressor);
                break;
            }
        }
        return true;
    }

    // FOK feasibility check: sums resting quantity available to trade against
    // `incoming`, excluding levels/orders that would be blocked by self-trade
    // prevention (since that liquidity cannot actually be consumed).
    bool canFullyFillLocked(const Order& incoming) const {
        long long available = 0;
        if (incoming.side == Side::Buy) {
            for (const auto& [price, level] : sellLevels_) {
                if (incoming.type != OrderType::Market && price > incoming.price) {
                    break;
                }
                for (const auto* order : level) {
                    if (order->clientId != incoming.clientId) {
                        available += order->quantity;
                    }
                }
                if (available >= incoming.quantity) {
                    return true;
                }
            }
        } else {
            for (auto it = buyLevels_.rbegin(); it != buyLevels_.rend(); ++it) {
                if (incoming.type != OrderType::Market && it->first < incoming.price) {
                    break;
                }
                for (const auto* order : it->second) {
                    if (order->clientId != incoming.clientId) {
                        available += order->quantity;
                    }
                }
                if (available >= incoming.quantity) {
                    return true;
                }
            }
        }
        return available >= incoming.quantity;
    }

    void executeMarketOrderLocked(Order& incoming, std::vector<TradeRecord>& newTrades, std::vector<Order>& touchedOrders) {
        while (incoming.quantity > 0 && incoming.active) {
            Order* counterparty = (incoming.side == Side::Buy) ? getBestSellOrderLocked() : getBestBuyOrderLocked();
            if (!counterparty) {
                break;
            }
            if (applySelfTradePreventionLocked(incoming, counterparty, touchedOrders)) {
                if (!incoming.active) {
                    break; // aggressor was cancelled by STP policy
                }
                continue; // resting counterparty removed; retry with next best
            }

            const int tradeQty = std::min(incoming.quantity, counterparty->quantity);
            const double tradePrice = counterparty->price;
            recordTradeLocked(incoming, *counterparty, tradeQty, tradePrice, newTrades);

            incoming.quantity -= tradeQty;
            counterparty->quantity -= tradeQty;
            if (counterparty->quantity == 0) {
                counterparty->active = false;
                counterparty->status = OrderStatus::Filled;
                removeFromBookLocked(counterparty);
                clientOrderKeys_.erase(clientOrderKey(counterparty->clientId, counterparty->clientOrderId));
                --activeCount_;
            } else {
                counterparty->status = OrderStatus::PartiallyFilled;
            }
            touchedOrders.push_back(*counterparty);
            stats_.onOrderMatched();
        }
        if (incoming.active) {
            if (incoming.quantity > 0) {
                // Unfilled remainder of a market order cannot rest; cancel it.
                incoming.active = false;
                incoming.status = OrderStatus::Cancelled;
                clientOrderKeys_.erase(clientOrderKey(incoming.clientId, incoming.clientOrderId));
                --activeCount_;
            } else {
                incoming.active = false;
                incoming.status = OrderStatus::Filled;
                clientOrderKeys_.erase(clientOrderKey(incoming.clientId, incoming.clientOrderId));
                --activeCount_;
            }
        }
    }

    // Generic crossing loop, re-evaluated after any book mutation that could
    // create or extend a cross (new limit order added, or an order modified).
    // `aggressor` identifies which side of any given cross is the order that
    // triggered this pass, which matters for self-trade-prevention policy.
    void matchOrdersLocked(Order* aggressor, std::vector<TradeRecord>& newTrades, std::vector<Order>& touchedOrders) {
        while (true) {
            Order* bestBuy = getBestBuyOrderLocked();
            Order* bestSell = getBestSellOrderLocked();
            if (!bestBuy || !bestSell) {
                break;
            }
            const bool crosses = bestBuy->type == OrderType::Market || bestSell->type == OrderType::Market
                                  || bestBuy->price >= bestSell->price;
            if (!crosses) {
                break;
            }

            Order* restingCounterparty = (bestBuy == aggressor) ? bestSell : bestBuy;
            if (applySelfTradePreventionLocked(*aggressor, restingCounterparty, touchedOrders)) {
                if (!aggressor->active) {
                    break;
                }
                continue;
            }

            const int tradeQty = std::min(bestBuy->quantity, bestSell->quantity);
            const double tradePrice = bestSell->price; // resting (maker) side sets the price
            recordTradeLocked(*bestBuy, *bestSell, tradeQty, tradePrice, newTrades);

            bestBuy->quantity -= tradeQty;
            bestSell->quantity -= tradeQty;

            if (bestBuy->quantity == 0) {
                bestBuy->active = false;
                bestBuy->status = OrderStatus::Filled;
                removeFromBookLocked(bestBuy);
                clientOrderKeys_.erase(clientOrderKey(bestBuy->clientId, bestBuy->clientOrderId));
                --activeCount_;
                stats_.onOrderMatched();
            } else {
                bestBuy->status = OrderStatus::PartiallyFilled;
            }
            if (bestSell->quantity == 0) {
                bestSell->active = false;
                bestSell->status = OrderStatus::Filled;
                removeFromBookLocked(bestSell);
                clientOrderKeys_.erase(clientOrderKey(bestSell->clientId, bestSell->clientOrderId));
                --activeCount_;
                stats_.onOrderMatched();
            } else {
                bestSell->status = OrderStatus::PartiallyFilled;
            }
            touchedOrders.push_back(*bestBuy);
            touchedOrders.push_back(*bestSell);
        }
    }

    void recordTradeLocked(Order& buyOrder, Order& sellOrder, int quantity, double price, std::vector<TradeRecord>& newTrades) {
        TradeRecord trade;
        trade.tradeId = ++tradeCounter_;
        trade.buyOrderId = buyOrder.side == Side::Buy ? buyOrder.id : sellOrder.id;
        trade.sellOrderId = buyOrder.side == Side::Buy ? sellOrder.id : buyOrder.id;
        trade.symbol = buyOrder.symbol;
        trade.price = price;
        trade.quantity = quantity;
        trade.timestamp = currentTimestamp();
        tradeHistory_.push_back(trade);
        newTrades.push_back(std::move(trade));
    }

    static std::string currentTimestamp() {
        const auto now = std::chrono::system_clock::now();
        const auto time = std::chrono::system_clock::to_time_t(now);
        std::tm localTime{};
        localtime_r(&time, &localTime);
        std::ostringstream stream;
        stream << std::put_time(&localTime, "%Y-%m-%d %H:%M:%S");
        return stream.str();
    }

    ThreadSafeLogger& logger_;
    OrderBookStats& stats_;
    AsyncPersistenceStore& persistence_;
    SelfTradePolicy stpPolicy_;

    mutable std::mutex mutex_;
    // Orders are stored BY VALUE keyed by id. unordered_map guarantees
    // pointer/reference stability across insert/rehash as long as the
    // element itself is not erased, so pointers cached in buyLevels_/
    // sellLevels_ remain valid for the lifetime of the order.
    std::unordered_map<std::uint64_t, Order> orders_;
    std::unordered_set<std::string> clientOrderKeys_; // "clientId\x1fclientOrderId"
    std::map<double, std::deque<Order*>, std::greater<double>> buyLevels_;
    std::map<double, std::deque<Order*>, std::less<double>> sellLevels_;
    std::vector<TradeRecord> tradeHistory_;
    std::size_t activeCount_ = 0;

    std::uint64_t idCounter_ = 0;
    std::uint64_t prioritySequence_ = 0;
    std::uint64_t tradeCounter_ = 0;
    std::atomic<std::uint64_t> clientSequenceCounter_{0}; // lock-free: used before mutex_ is taken
};

// ----------------------------------------------------------------------------
// Request handed from the TCP gateway to a worker thread.
// ----------------------------------------------------------------------------

struct OrderRequest {
    std::string payload;
    std::string clientIp;
    std::shared_ptr<std::promise<std::string>> promise;
};

// ----------------------------------------------------------------------------
// RAII socket handle: guarantees close() exactly once, even on early return
// or exception, and is move-only so ownership is always unambiguous.
// ----------------------------------------------------------------------------

class SocketHandle {
public:
    SocketHandle() = default;
    explicit SocketHandle(int fd) : fd_(fd) {}
    ~SocketHandle() { reset(); }

    SocketHandle(const SocketHandle&) = delete;
    SocketHandle& operator=(const SocketHandle&) = delete;

    SocketHandle(SocketHandle&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
    SocketHandle& operator=(SocketHandle&& other) noexcept {
        if (this != &other) {
            reset();
            fd_ = other.fd_;
            other.fd_ = -1;
        }
        return *this;
    }

    int get() const { return fd_; }
    bool valid() const { return fd_ >= 0; }

    void reset() {
        if (fd_ >= 0) {
            shutdown(fd_, SHUT_RDWR);
            close(fd_);
            fd_ = -1;
        }
    }

private:
    int fd_ = -1;
};

// ----------------------------------------------------------------------------
// TCP gateway.
//
// Wire protocol: newline-delimited messages, one request/response pair per
// connection (client connects, sends one '\n'-terminated message, receives
// one '\n'-terminated response, connection closes). recv() is called in a
// loop until either a newline is found, the peer closes the connection, or a
// maximum message size is exceeded, so a message split across multiple TCP
// segments is handled correctly. send() is also looped to cover partial
// writes. Both socket directions have timeouts so a slow/malicious client
// cannot pin a handler thread forever. Client-handling threads are tracked
// and joined in stop() so no thread ever outlives the objects it references.
// ----------------------------------------------------------------------------

class TcpOrderGateway {
public:
    TcpOrderGateway(ThreadSafeLogger& logger, ThreadSafeQueue<OrderRequest>& queue)
        : logger_(logger), queue_(queue) {}

    ~TcpOrderGateway() { stop(); }

    void start(int port = 5556) {
        port_ = port;
        running_.store(true, std::memory_order_release);
        acceptThread_ = std::thread([this, port] { runServer(port); });
    }

    void stop() {
        running_.store(false, std::memory_order_release);
        serverSocket_.reset();
        if (acceptThread_.joinable()) {
            acceptThread_.join();
        }
        // Join every still-running client handler thread; without this the
        // original implementation's detached threads could outlive (and
        // dereference) objects that are destroyed right after stop() returns.
        std::lock_guard<std::mutex> lock(clientThreadsMutex_);
        for (auto& thread : clientThreads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }
        clientThreads_.clear();
        logger_.logShutdown("gateway shutdown complete");
    }

    void sendOrderMessage(const std::string& message) const {
        for (int attempt = 0; attempt < 10; ++attempt) {
            SocketHandle client(socket(AF_INET, SOCK_STREAM, 0));
            if (!client.valid()) {
                logger_.logError("tcp", "socket creation failed for outbound test message");
                return;
            }
            sockaddr_in serverAddress{};
            serverAddress.sin_family = AF_INET;
            serverAddress.sin_port = htons(port_);
            inet_pton(AF_INET, "127.0.0.1", &serverAddress.sin_addr);
            if (connect(client.get(), reinterpret_cast<sockaddr*>(&serverAddress), sizeof(serverAddress)) < 0) {
                std::this_thread::sleep_for(50ms);
                continue;
            }
            const std::string framed = message + "\n";
            sendAll(client.get(), framed);
            return;
        }
        logger_.logError("tcp", "failed to connect to own gateway for outbound test message");
    }

private:
    static constexpr std::size_t kMaxMessageSize = 8192;
    static constexpr int kMaxConcurrentClients = 512;

    void runServer(int port) {
        SocketHandle listener(socket(AF_INET, SOCK_STREAM, 0));
        if (!listener.valid()) {
            logger_.logError("tcp", "socket creation failed");
            return;
        }
        int opt = 1;
        setsockopt(listener.get(), SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = INADDR_ANY;
        address.sin_port = htons(port);
        if (bind(listener.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
            logger_.logError("tcp", "bind failed");
            return;
        }
        if (listen(listener.get(), 128) < 0) {
            logger_.logError("tcp", "listen failed");
            return;
        }

        serverSocket_ = std::move(listener);
        logger_.logShutdown("order gateway listening on port " + std::to_string(port));

        while (running_.load(std::memory_order_acquire)) {
            sockaddr_in clientAddress{};
            socklen_t clientLength = sizeof(clientAddress);
            int rawClientSocket = accept(serverSocket_.get(), reinterpret_cast<sockaddr*>(&clientAddress), &clientLength);
            if (rawClientSocket < 0) {
                if (!running_.load(std::memory_order_acquire)) {
                    break;
                }
                logger_.logWarning("tcp", "accept failed");
                continue;
            }

            char clientIpBuffer[INET_ADDRSTRLEN]{};
            inet_ntop(AF_INET, &clientAddress.sin_addr, clientIpBuffer, sizeof(clientIpBuffer));
            const std::string clientIp(clientIpBuffer);
            logger_.logClientConnected(clientIp);

            if (activeClients_.load(std::memory_order_acquire) >= kMaxConcurrentClients) {
                // Backpressure: refuse rather than let unbounded threads pile up.
                logger_.logWarning("tcp", "connection limit reached, rejecting client " + clientIp);
                SocketHandle reject(rawClientSocket);
                sendAll(reject.get(), "ERROR|OVERLOAD|too many concurrent connections\n");
                continue;
            }

            activeClients_.fetch_add(1, std::memory_order_relaxed);
            SocketHandle clientSocket(rawClientSocket);
            std::lock_guard<std::mutex> lock(clientThreadsMutex_);
            // Reap finished threads opportunistically so the tracking vector
            // doesn't grow without bound over a long-running server.
            clientThreads_.erase(
                std::remove_if(clientThreads_.begin(), clientThreads_.end(),
                                [](std::thread& t) { return !t.joinable(); }),
                clientThreads_.end());
            clientThreads_.emplace_back([this, sock = std::move(clientSocket), clientIp]() mutable {
                handleClient(std::move(sock), clientIp);
                activeClients_.fetch_sub(1, std::memory_order_relaxed);
            });
        }
    }

    void handleClient(SocketHandle clientSocket, const std::string& clientIp) {
        // 5 second timeout on both directions: a stalled or malicious client
        // cannot pin this thread (and thus a queue slot) indefinitely.
        timeval timeout{5, 0};
        setsockopt(clientSocket.get(), SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        setsockopt(clientSocket.get(), SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

        std::optional<std::string> message = readFramedMessage(clientSocket.get());
        if (!message.has_value()) {
            logger_.logWarning("tcp", "no valid framed message received from client " + clientIp);
            logger_.logClientDisconnected(clientIp);
            return;
        }

        logger_.logOrderReceived(*message, clientIp);

        auto promise = std::make_shared<std::promise<std::string>>();
        auto future = promise->get_future();
        queue_.push(OrderRequest{std::move(*message), clientIp, promise});

        // Bound the wait so a stuck matching pipeline cannot hang this thread
        // forever; on timeout we report an error rather than blocking.
        std::string response;
        if (future.wait_for(10s) == std::future_status::ready) {
            response = future.get() + "\n";
        } else {
            response = "ERROR|TIMEOUT|processing did not complete in time\n";
        }
        sendAll(clientSocket.get(), response);
        logger_.logClientDisconnected(clientIp);
    }

    // Reads until '\n' is found, the peer closes the connection, or the
    // message exceeds kMaxMessageSize. Correctly handles partial recv().
    std::optional<std::string> readFramedMessage(int fd) const {
        std::string buffer;
        buffer.reserve(256);
        char chunk[512];
        while (buffer.size() <= kMaxMessageSize) {
            const ssize_t bytesRead = recv(fd, chunk, sizeof(chunk), 0);
            if (bytesRead < 0) {
                return std::nullopt; // error or timeout
            }
            if (bytesRead == 0) {
                // Peer closed. Accept whatever was buffered if it looks complete.
                break;
            }
            buffer.append(chunk, static_cast<std::size_t>(bytesRead));
            auto newlinePos = buffer.find('\n');
            if (newlinePos != std::string::npos) {
                buffer.resize(newlinePos);
                return buffer;
            }
        }
        if (buffer.empty() || buffer.size() > kMaxMessageSize) {
            return std::nullopt;
        }
        return buffer;
    }

    static void sendAll(int fd, const std::string& data) {
        std::size_t sent = 0;
        while (sent < data.size()) {
            const ssize_t result = send(fd, data.data() + sent, data.size() - sent, 0);
            if (result <= 0) {
                return; 
            }
            sent += static_cast<std::size_t>(result);
        }
    }

    ThreadSafeLogger& logger_;
    ThreadSafeQueue<OrderRequest>& queue_;
    std::thread acceptThread_;
    std::atomic<bool> running_{false};
    SocketHandle serverSocket_;
    int port_ = 5556;

    std::mutex clientThreadsMutex_;
    std::vector<std::thread> clientThreads_;
    std::atomic<int> activeClients_{0};
};

// ----------------------------------------------------------------------------
// Simulates concurrent order flow to exercise the matching engine end to end.
// ----------------------------------------------------------------------------

void runSimulation(OrderBook& book, ThreadSafeLogger& logger) {
    std::vector<std::thread> workers;
    workers.reserve(8);
    for (int i = 0; i < 8; ++i) {
        workers.emplace_back([&book, i] {
            for (int j = 0; j < 125; ++j) {
                const bool isBuy = ((i + j) % 2) == 0;
                const std::string symbol = (j % 3 == 0) ? "AAPL" : "MSFT";
                const double price = 100.0 + static_cast<double>((i + j) % 10);
                const int qty = 1 + (j % 5);
                const std::string clientOrderId = "sim-" + std::to_string(i) + "-" + std::to_string(j);
                const std::string message = symbol + "|" + (isBuy ? "BUY" : "SELL") + "|" +
                                             std::to_string(price) + "|" + std::to_string(qty) + "|LIMIT|" + clientOrderId;
                book.processMessage(message, "127.0.0.1-sim" + std::to_string(i));
            }
        });
    }
    for (auto& worker : workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    logger.logWarning("simulation", "completed concurrent order simulation");
}

int main() {
    ThreadSafeLogger logger;
    AsyncPersistenceStore persistence(logger);
    if (!persistence.connect()) {
        logger.logWarning("main", "PostgreSQL unavailable; continuing in-memory mode");
    } else {
        persistence.start();
    }

    OrderBookStats stats;
    OrderBook book(logger, stats, persistence, SelfTradePolicy::CancelResting);
    ThreadSafeQueue<OrderRequest> queue;
    TcpOrderGateway gateway(logger, queue);

    gateway.start(5556);
    std::this_thread::sleep_for(100ms);

    // Worker pool: pulls requests off the queue and runs them through the
    // matching engine. Started before traffic so nothing blocks unexpectedly.
    std::vector<std::thread> workers;
    workers.reserve(4);
    for (int i = 0; i < 4; ++i) {
        workers.emplace_back([&book, &queue, &stats] {
            while (true) {
                OrderRequest request;
                if (!queue.pop(request)) {
                    break;
                }
                stats.setQueueSize(queue.size());
                try {
                    const auto result = book.processMessage(request.payload, request.clientIp);
                    if (request.promise) {
                        request.promise->set_value(result.response);
                    }
                } catch (const std::exception& ex) {
                    // A worker thread must never die from an unexpected
                    // exception -- that would silently shrink the pool and
                    // eventually stall the whole gateway.
                    if (request.promise) {
                        request.promise->set_value(std::string("ERROR|INTERNAL|") + ex.what());
                    }
                }
            }
        });
    }

    gateway.sendOrderMessage("AAPL|BUY|100|10|LIMIT|client-001");
    gateway.sendOrderMessage("AAPL|SELL|102|5|LIMIT|client-002");
    gateway.sendOrderMessage("AAPL|BUY|0|12|MARKET|client-003");
    gateway.sendOrderMessage("IOC|AAPL|BUY|101|20|LIMIT|client-004");
    gateway.sendOrderMessage("FOK|AAPL|BUY|101|999999|LIMIT|client-005");

    std::this_thread::sleep_for(200ms);
    runSimulation(book, logger);

    gateway.stop();
    queue.shutdown();
    for (auto& worker : workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    const auto buyResult = book.processMessage("AAPL|BUY|101|8|LIMIT|client-101", "127.0.0.1");
    const auto sellResult = book.processMessage("AAPL|SELL|99|7|LIMIT|client-102", "127.0.0.1");
    book.modifyOrder(buyResult.orderId, 103.0, 6, "127.0.0.1");
    book.cancelOrder(sellResult.orderId, "127.0.0.1");

    const auto [bestBid, bestAsk] = book.getBestBidAsk();
    std::cout << "Best Bid/Ask: " << bestBid << "/" << bestAsk << std::endl;
    std::cout << "Market Depth" << std::endl;
    for (const auto& line : book.getMarketDepth()) {
        std::cout << line << std::endl;
    }

    const auto snapshot = stats.snapshot();
    std::cout << "Final Statistics" << std::endl;
    std::cout << "- Orders Received: " << snapshot.ordersReceived << std::endl;
    std::cout << "- Orders Accepted: " << snapshot.ordersAccepted << std::endl;
    std::cout << "- Orders Rejected: " << snapshot.ordersRejected << std::endl;
    std::cout << "- Orders Matched: " << snapshot.ordersMatched << std::endl;
    std::cout << "- Orders Modified: " << snapshot.ordersModified << std::endl;
    std::cout << "- Orders Cancelled: " << snapshot.ordersCancelled << std::endl;
    std::cout << "- Trades Executed: " << snapshot.tradesExecuted << std::endl;
    std::cout << "- Queue Size: " << snapshot.queueSize << std::endl;
    std::cout << "- Average Matching Latency (ms): " << snapshot.averageMatchingLatencyMs << std::endl;
    std::cout << "- Maximum Matching Latency (ms): " << snapshot.maxMatchingLatencyMs << std::endl;
    std::cout << "- Orders Per Second: " << snapshot.ordersPerSecond << std::endl;
    std::cout << "- Trades Per Second: " << snapshot.tradesPerSecond << std::endl;
    std::cout << "- Active Orders: " << snapshot.activeOrders << std::endl;
    std::cout << "- Cancel Rate: " << snapshot.cancelRate << std::endl;
    std::cout << "- Fill Rate: " << snapshot.fillRate << std::endl;

    persistence.stop();
    persistence.disconnect();
    return 0;
}