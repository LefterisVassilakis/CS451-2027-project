#pragma once

#include "socket_wrapper.hpp"
#include "parser.hpp"
#include <cstdint>
#include <functional>
#include <vector>
#include <set>
#include <cstring>
#include <stdexcept>
#include <thread>
#include <atomic>
#include <map>
#include <mutex>
#include <condition_variable>
#include <chrono>

static constexpr size_t MAX_PACKET_SIZE = 65507; // Maximum size for a UDP packet payload
static constexpr size_t OFFSET_TYPE   = 0;
static constexpr size_t OFFSET_SENDER = 1;
static constexpr size_t OFFSET_COUNT  = 2;
static constexpr size_t OFFSET_SEQ    = 3;
static constexpr size_t OFFSET_LEN    = 7;
static constexpr size_t OFFSET_ACK    = 7;
static constexpr size_t HEADER_SIZE   = 11;
static constexpr size_t ACK_PACKET_SIZE = 11;
static constexpr size_t MAX_MESSAGE_SIZE = MAX_PACKET_SIZE - HEADER_SIZE;
static constexpr size_t WINDOW_SIZE = 1000;

static constexpr std::chrono::milliseconds INITIAL_TIMEOUT = std::chrono::milliseconds(500);
static constexpr std::chrono::milliseconds MAX_TIMEOUT = std::chrono::milliseconds(4000);
static constexpr std::chrono::milliseconds RETRANSMIT_TICK = std::chrono::milliseconds(50);


enum class PacketType : uint8_t {
    DATA = 0,
    ACK = 1
};

struct DedupState{
    uint32_t lowestDeliveredSeq = 0;
    uint32_t highestDeliveredSeq = 0;
    std::set<uint32_t> missingSeqs;
};

struct UnackedMessage {
    std::vector<uint8_t> payload;
    std::chrono::steady_clock::time_point lastSent;
    std::chrono::milliseconds timeout;
};

struct PendingRetransmit {
    size_t peer;
    uint32_t seq;
    std::vector<uint8_t> payload;
};


class PerfectLinks {
    public:
        using DeliverCallback = std::function<void(uint32_t senderId, const uint8_t* data, size_t len)>;

        PerfectLinks(unsigned long id, const std::vector<Parser::Host>& hosts, DeliverCallback onDeliver)
            : socket_(hosts[id-1].portReadable()),
              deliver_(std::move(onDeliver)),
              id_(id),
              nextSeq_(hosts.size(), 1),
              unacked_(hosts.size()) { // possibly changed to a random ISN

                for (const auto& host : hosts) {
                    sockaddr_in addr{};
                    addr.sin_family = AF_INET;
                    addr.sin_addr.s_addr = host.ip;
                    addr.sin_port = host.port;
                    peers_.push_back(addr);
                    dedupStates_.emplace_back();
                }
        }

        bool SendMessage(unsigned long receiverId, const uint8_t* data, size_t len) {

            if (stopped_) return false;

            if  (len > MAX_MESSAGE_SIZE) {
                throw std::invalid_argument("Message length exceeds maximum allowed size.");
            }

            std::unique_lock<std::mutex> lock(unackedMutex_);
            windowCv_.wait(lock, [&] {
                return stopped_ || unacked_[receiverId-1].size() < WINDOW_SIZE;
            });
            if (stopped_) return false;
                uint32_t seq = nextSeq_[receiverId-1]++;
                unacked_[receiverId-1].emplace(seq, UnackedMessage{std::vector<uint8_t>(data, data+len),
                    std::chrono::steady_clock::now(), INITIAL_TIMEOUT});
            lock.unlock();

            uint8_t buf[MAX_PACKET_SIZE];
            size_t packetLen = BuildDataPacket(buf, seq, data, len);
            socket_.SendPacket(buf, packetLen, peers_[receiverId - 1]);

            return true;
        }

        void Start(){
            receiverThread_ = std::thread(&PerfectLinks::ReceiveLoop, this);
            receiverThread_.detach();
            retransmitThread_ = std::thread(&PerfectLinks::RetransmitLoop, this);
            retransmitThread_.detach();
        }

        unsigned long Id() const {
            return id_;
        }

        void Stop() {
            stopped_ = true;
            windowCv_.notify_all();
        }

    private:
        SocketWrapper socket_;
        DeliverCallback deliver_;
        unsigned long id_;
        std::vector<sockaddr_in> peers_;
        std::vector<uint32_t> nextSeq_;
        std::thread receiverThread_;
        std::vector<DedupState> dedupStates_;
        std::atomic<bool> stopped_{false};
        std::vector<std::map<uint32_t, UnackedMessage>> unacked_;
        std::mutex unackedMutex_;
        std::condition_variable windowCv_;
        std::thread retransmitThread_;

        void DeliverMessage(uint32_t senderId, const uint8_t* data, size_t len) {
            deliver_(senderId, data, len);
        }

        void ReceiveLoop() {
            uint8_t buf[MAX_PACKET_SIZE];
            sockaddr_in from;
            while (true) {
                ssize_t bytesReceived = socket_.ReceivePacket(buf, sizeof(buf), from);
                if (stopped_) return;
                if (bytesReceived < 0) {
                    continue;
                }

                if (static_cast<size_t>(bytesReceived) < HEADER_SIZE) {
                    continue;
                }

                PacketType type = static_cast<PacketType>(buf[OFFSET_TYPE]);

                if (type == PacketType::DATA) {
                    uint32_t senderId = buf[OFFSET_SENDER];
                    uint32_t seq;
                    std::memcpy(&seq, buf + OFFSET_SEQ, sizeof(seq));
                    seq = ntohl(seq);
                    uint32_t len;
                    std::memcpy(&len, buf + OFFSET_LEN, sizeof(len));
                    len = ntohl(len);

                    if (len > MAX_MESSAGE_SIZE || HEADER_SIZE + len > static_cast<size_t>(bytesReceived)) {
                        continue;
                    }

                    if (senderId < 1 || senderId > dedupStates_.size()) {
                        continue;
                    }
                    
                    if (IsNewMessage(senderId, seq)) {
                        DeliverMessage(senderId, buf + HEADER_SIZE, len);
                    }

                    if (stopped_) return;

                    uint32_t ack = dedupStates_[senderId-1].lowestDeliveredSeq;
                    uint8_t ackBuf[ACK_PACKET_SIZE];
                    size_t ackLen = BuildAckPacket(ackBuf, seq, ack);
                    socket_.SendPacket(ackBuf, ackLen, peers_[senderId-1]);

                } else if (type == PacketType::ACK){
                    uint32_t ackerId = buf[OFFSET_SENDER];
                    if (ackerId <= unacked_.size() && ackerId >= 1){
                        if (bytesReceived == ACK_PACKET_SIZE){
                            uint32_t seq;
                            std::memcpy(&seq, buf + OFFSET_SEQ, sizeof(seq));
                            seq = ntohl(seq);
                            uint32_t ack;
                            std::memcpy(&ack, buf + OFFSET_LEN, sizeof(ack));
                            ack = ntohl(ack);

                            {
                                std::lock_guard<std::mutex> lock(unackedMutex_);
                                auto& pending = unacked_[ackerId - 1];
                                pending.erase(pending.begin(), pending.upper_bound(ack));
                                pending.erase(seq);
                            }

                            windowCv_.notify_one();

                        }
                    }
                }
            }
        }

        bool IsNewMessage(uint32_t senderId, uint32_t seq) {
            DedupState& state = dedupStates_[senderId - 1];

            if (seq <= state.lowestDeliveredSeq) {
                return false;
            }
            if (seq <= state.highestDeliveredSeq) {
                if (state.missingSeqs.erase(seq) == 0) {
                    return false;
                }
            } else {
                for (uint32_t s = state.highestDeliveredSeq + 1; s < seq; ++s) {
                    state.missingSeqs.insert(s);
                }
                state.highestDeliveredSeq = seq;
            }
            state.lowestDeliveredSeq = state.missingSeqs.empty()
                ? state.highestDeliveredSeq
                : *state.missingSeqs.begin() - 1;

            return true;
        }

        size_t BuildDataPacket(uint8_t* buf, uint32_t seq, const uint8_t* data, size_t len){
            buf[OFFSET_TYPE] = static_cast<uint8_t>(PacketType::DATA);
            buf[OFFSET_SENDER] = static_cast<uint8_t>(id_);
            buf[OFFSET_COUNT] = 1; // message count
            uint32_t seqNet = htonl(seq);
            std::memcpy(buf + OFFSET_SEQ, &seqNet, sizeof(seqNet));
            uint32_t lenNet = htonl(static_cast<uint32_t>(len));
            std::memcpy(buf + OFFSET_LEN, &lenNet, sizeof(lenNet));
            std::memcpy(buf + HEADER_SIZE, data, len);

            return HEADER_SIZE + len;
        }

        size_t BuildAckPacket(uint8_t* buf, uint32_t seq, uint32_t ack){
            buf[OFFSET_TYPE] = static_cast<uint8_t>(PacketType::ACK);
            buf[OFFSET_SENDER] = static_cast<uint8_t>(id_);
            buf[OFFSET_COUNT] = 0;
            
            uint32_t seqNet = htonl(seq);
            std::memcpy(buf + OFFSET_SEQ, &seqNet, sizeof(seqNet));
            uint32_t ackNet = htonl(ack);
            std::memcpy(buf + OFFSET_ACK, &ackNet, sizeof(ackNet));

            return ACK_PACKET_SIZE;
        }

        void RetransmitLoop(){
            uint8_t buf[MAX_PACKET_SIZE];
            std::vector<PendingRetransmit> toResend;
            while (true) {
                std::this_thread::sleep_for(RETRANSMIT_TICK);
                if (stopped_) return;
                toResend.clear();
                {
                std::lock_guard<std::mutex> lock(unackedMutex_);
                auto now = std::chrono::steady_clock::now();
                for (size_t p = 0; p < unacked_.size(); ++p) {
                    for (auto& [seq, msg] : unacked_[p]) {
                        if (now - msg.lastSent >= msg.timeout) {
                            toResend.push_back({p, seq, msg.payload});
                            msg.lastSent = now;
                            msg.timeout = std::min(msg.timeout * 2, MAX_TIMEOUT);
                        }
                    }
                }
            }

            for (const auto& item : toResend) {
                if (stopped_) return;
                size_t len = BuildDataPacket(buf, item.seq, item.payload.data(), item.payload.size());
                socket_.SendPacket(buf, len, peers_[item.peer]);
            }

    }
        }
};