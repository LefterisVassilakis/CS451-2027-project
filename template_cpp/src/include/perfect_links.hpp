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

static constexpr size_t OFFSET_TYPE   = 0;
static constexpr size_t OFFSET_SENDER = 1;
static constexpr size_t OFFSET_COUNT  = 2;
static constexpr size_t PACKET_HEADER_SIZE = 3;

static constexpr size_t ENTRY_OFFSET_SEQ = 0;
static constexpr size_t ENTRY_OFFSET_LEN = 4;
static constexpr size_t ENTRY_HEADER_SIZE = 8;

static constexpr size_t MAX_PACKET_SIZE = 65507; // Maximum size for a UDP packet payload
static constexpr size_t MAX_BATCH = 8;
static constexpr size_t MAX_MESSAGE_SIZE = MAX_PACKET_SIZE - PACKET_HEADER_SIZE - ENTRY_HEADER_SIZE;
static constexpr size_t WINDOW_SIZE = 1000;

static constexpr size_t ACK_OFFSET_CUM = 3;
static constexpr size_t ACK_OFFSET_SEQS = 7;
static constexpr size_t ACK_HEADER_SIZE = 7;
static constexpr size_t MAX_ACK_SIZE = ACK_HEADER_SIZE + 4 * MAX_BATCH;


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
              unacked_(hosts.size()),
              pending_(hosts.size()) {

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

            size_t q = receiverId - 1;
            std::vector<uint8_t> out;

            std::unique_lock<std::mutex> lock(unackedMutex_);
            while (!stopped_ && unacked_[q].size() >= WINDOW_SIZE) {
                if (!pending_[q].empty()) {
                    out.swap(pending_[q]);
                    lock.unlock();
                    if (!stopped_) {
                        socket_.SendPacket(out.data(), out.size(), peers_[q]);
                    }
                    out.clear();
                    lock.lock();
                    continue;
                }
                windowCv_.wait(lock);
            }
            
            if (stopped_) return false;

            uint32_t seq = nextSeq_[q]++;
            unacked_[q].emplace(seq, UnackedMessage{std::vector<uint8_t>(data, data + len),
                    std::chrono::steady_clock::now(), INITIAL_TIMEOUT});
            AppendToPending(q, seq, data, len, out);
            lock.unlock();
            
            if (!out.empty() && !stopped_){
                socket_.SendPacket(out.data(), out.size(), peers_[q]);
            }

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
        std::vector<std::vector<uint8_t>> pending_;
        std::mutex unackedMutex_;
        std::condition_variable windowCv_;
        std::thread retransmitThread_;

        void DeliverMessage(uint32_t senderId, const uint8_t* data, size_t len) {
            deliver_(senderId, data, len);
        }

        void HandleData(const uint8_t* buf, size_t size){
            uint32_t senderId = buf[OFFSET_SENDER];
            if (senderId < 1 || senderId > dedupStates_.size()) return;

            size_t count = buf[OFFSET_COUNT];
            if (count < 1 || count > MAX_BATCH) return;

            size_t offset = PACKET_HEADER_SIZE;
            for (size_t i = 0; i < count; i++){
                if (size - offset < ENTRY_HEADER_SIZE) return;

                uint32_t lenNet;
                std::memcpy(&lenNet, buf + offset + ENTRY_OFFSET_LEN, sizeof(lenNet));
                uint32_t len = ntohl(lenNet);
                if (len > size - offset - ENTRY_HEADER_SIZE) return;

                offset += ENTRY_HEADER_SIZE + len;
            }

            if (offset != size) return;

            offset = PACKET_HEADER_SIZE;
            uint32_t seqs[MAX_BATCH];

            for (size_t i = 0; i < count; i++){
                uint32_t seqNet;
                std::memcpy(&seqNet, buf + offset + ENTRY_OFFSET_SEQ, sizeof(seqNet));
                uint32_t seq = ntohl(seqNet);

                uint32_t lenNet;
                std::memcpy(&lenNet, buf + offset + ENTRY_OFFSET_LEN, sizeof(lenNet));
                uint32_t len = ntohl(lenNet);

                const uint8_t* payload = buf + offset + ENTRY_HEADER_SIZE;
                if (IsNewMessage(senderId, seq)){
                    DeliverMessage(senderId, payload, len);
                }

                seqs[i] = seq;
                offset += ENTRY_HEADER_SIZE + len;
            }

            if (stopped_) return;

            uint32_t cumAck = dedupStates_[senderId-1].lowestDeliveredSeq;
            uint8_t ackBuf[MAX_ACK_SIZE];
            size_t ackLen = BuildAckPacket(ackBuf, cumAck, seqs, count);
            socket_.SendPacket(ackBuf, ackLen, peers_[senderId-1]);
        }

        void HandleAck(const uint8_t* buf, size_t size){
            uint32_t ackerId = buf[OFFSET_SENDER];
            if (ackerId < 1 || ackerId > unacked_.size()) return;

            if (size < ACK_HEADER_SIZE) return;

            size_t k = buf[OFFSET_COUNT];
            if (k < 1 || k > MAX_BATCH) return;

            if (size != ACK_HEADER_SIZE + sizeof(uint32_t) * k) return;

            uint32_t cumAckNet;
            std::memcpy(&cumAckNet, buf + ACK_OFFSET_CUM, sizeof(cumAckNet));
            uint32_t cumAck = ntohl(cumAckNet);

            uint32_t seqs[MAX_BATCH];
            for (size_t i = 0; i < k; i++){
                uint32_t seqNet;
                std::memcpy(&seqNet, buf + ACK_OFFSET_SEQS + sizeof(uint32_t) * i, sizeof(seqNet));
                seqs[i] = ntohl(seqNet);
            }

            {
                std::lock_guard<std::mutex> lock(unackedMutex_);
                auto& pending = unacked_[ackerId - 1];
                pending.erase(pending.begin(), pending.upper_bound(cumAck));
                for (size_t i = 0; i < k; i++){
                    pending.erase(seqs[i]);
                }
            }

            windowCv_.notify_one();

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

                size_t size = static_cast<size_t>(bytesReceived);

                if (size < PACKET_HEADER_SIZE) {
                    continue;
                }

                PacketType type = static_cast<PacketType>(buf[OFFSET_TYPE]);

                if (type == PacketType::DATA) {

                    HandleData(buf, size);

                } else if (type == PacketType::ACK){

                    HandleAck(buf, size);

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

        size_t StartDataPacket(uint8_t* buf){
            buf[OFFSET_TYPE] = static_cast<uint8_t>(PacketType::DATA);
            buf[OFFSET_SENDER] = static_cast<uint8_t>(id_);
            buf[OFFSET_COUNT] = 0;
            return PACKET_HEADER_SIZE;
        }

        size_t AppendEntry(uint8_t* buf, size_t packetLen,uint32_t seq, const uint8_t* data, size_t len){
            uint8_t* entry = buf + packetLen;
            uint32_t seqNet = htonl(seq);
            std::memcpy(entry + ENTRY_OFFSET_SEQ, &seqNet, sizeof(seqNet));
            uint32_t lenNet = htonl(static_cast<uint32_t>(len));
            std::memcpy(entry + ENTRY_OFFSET_LEN, &lenNet, sizeof(lenNet));
            std::memcpy(entry + ENTRY_HEADER_SIZE, data, len);
            buf[OFFSET_COUNT] = static_cast<uint8_t>(buf[OFFSET_COUNT] + 1);
            return packetLen + ENTRY_HEADER_SIZE + len;
        }


        size_t BuildAckPacket(uint8_t* buf, uint32_t cumAck, const uint32_t* seqs, size_t k){
            buf[OFFSET_TYPE] = static_cast<uint8_t>(PacketType::ACK);
            buf[OFFSET_SENDER] = static_cast<uint8_t>(id_);
            buf[OFFSET_COUNT] = static_cast<uint8_t>(k);

            uint32_t cumAckNet = htonl(cumAck);
            std::memcpy(buf + ACK_OFFSET_CUM, &cumAckNet, sizeof(cumAckNet));

            for(size_t i = 0; i < k; i++){
                uint32_t seqNet = htonl(seqs[i]);
                std::memcpy(buf + ACK_OFFSET_SEQS + sizeof(uint32_t) * i, &seqNet, sizeof(seqNet));
            }

            return ACK_HEADER_SIZE + sizeof(uint32_t) * k;
        }

        void RetransmitLoop(){
            uint8_t buf[MAX_PACKET_SIZE];
            std::vector<PendingRetransmit> toResend;
            std::vector<std::pair<size_t, std::vector<uint8_t>>> toFlush;

            while (true) {
                std::this_thread::sleep_for(RETRANSMIT_TICK);
                if (stopped_) return;
                toResend.clear();
                toFlush.clear();

                {
                    std::lock_guard<std::mutex> lock(unackedMutex_);
                    auto now = std::chrono::steady_clock::now();
                    for (size_t p = 0; p < unacked_.size(); ++p) {
                        if (!pending_[p].empty()){
                            toFlush.emplace_back(p, std::move(pending_[p]));
                            pending_[p].clear();
                        }
                        
                        for (auto& [seq, msg] : unacked_[p]) {
                            if (now - msg.lastSent >= msg.timeout) {
                                toResend.push_back({p, seq, msg.payload});
                                msg.lastSent = now;
                                msg.timeout = std::min(msg.timeout * 2, MAX_TIMEOUT);
                            }
                        }
                    }
                }

                for (const auto& [peer, packet] : toFlush) {
                    if (stopped_) return;
                    socket_.SendPacket(packet.data(), packet.size(), peers_[peer]);
                }


                size_t i = 0;
                while (i < toResend.size()){
                    if (stopped_) return;

                    size_t peer = toResend[i].peer;
                    size_t packetLen = StartDataPacket(buf);
                    size_t entries = 0;

                    while (i < toResend.size() && toResend[i].peer == peer &&
                        entries < MAX_BATCH && packetLen + ENTRY_HEADER_SIZE + toResend[i].payload.size() <= MAX_PACKET_SIZE){
                        const auto& payload = toResend[i].payload;
                        packetLen = AppendEntry(buf, packetLen, toResend[i].seq, payload.data(), payload.size());
                        entries++;
                        i++;
                    }

                    socket_.SendPacket(buf, packetLen, peers_[peer]);
                }
            }
        }

        void AppendToPending(size_t q, uint32_t seq, const uint8_t* data,
            size_t len, std::vector<uint8_t>& out){
            auto& p = pending_[q];

            if (!p.empty() && (p[OFFSET_COUNT] == MAX_BATCH ||
                p.size() + ENTRY_HEADER_SIZE + len > MAX_PACKET_SIZE)){
                    out.swap(p);
            }

            if (p.empty()){
                p.resize(PACKET_HEADER_SIZE);
                StartDataPacket(p.data());
            }

            size_t old = p.size();
            p.resize(old + ENTRY_HEADER_SIZE + len);
            AppendEntry(p.data(), old, seq, data, len);

            if (p[OFFSET_COUNT] == MAX_BATCH){
                out.swap(p);
            }
        }
};