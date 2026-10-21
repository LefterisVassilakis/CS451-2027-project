#include "socket_wrapper.hpp"
#include "parser.hpp"
#include <cstdint>
#include <functional>
#include <vector>
#include <cstring>
#include <stdexcept>
#include <thread>

static constexpr size_t MAX_PACKET_SIZE = 65507; // Maximum size for a UDP packet payload
static constexpr size_t OFFSET_TYPE   = 0;
static constexpr size_t OFFSET_SENDER = 1;
static constexpr size_t OFFSET_COUNT  = 2;
static constexpr size_t OFFSET_SEQ    = 3;
static constexpr size_t OFFSET_LEN    = 7;
static constexpr size_t HEADER_SIZE   = 11;
static constexpr size_t MAX_MESSAGE_SIZE = MAX_PACKET_SIZE - HEADER_SIZE;


enum class PacketType : uint8_t {
    DATA = 0,
    ACK = 1
};

class PerfectLinks {
    public:
        using DeliverCallback = std::function<void(uint32_t senderId, const uint8_t* data, size_t len)>;

        PerfectLinks(unsigned long id, const std::vector<Parser::Host>& hosts, DeliverCallback onDeliver)
            : socket_(hosts[id-1].portReadable()),
              deliver_(std::move(onDeliver)),
              id_(id),
              nextSeq_(hosts.size(), 1) { // possibly changed to a random ISN

                for (const auto& host : hosts) {
                    sockaddr_in addr{};
                    addr.sin_family = AF_INET;
                    addr.sin_addr.s_addr = host.ip;
                    addr.sin_port = host.port;
                    peers_.push_back(addr);
                }
        }

        void SendMessage(unsigned long receiverId, const uint8_t* data, size_t len) {

            if  (len > MAX_MESSAGE_SIZE) {
                throw std::invalid_argument("Message length exceeds maximum allowed size.");
            }

            uint8_t buf[MAX_PACKET_SIZE];
            buf[OFFSET_TYPE] = static_cast<uint8_t>(PacketType::DATA);
            buf[OFFSET_SENDER] = static_cast<uint8_t>(id_);
            buf[OFFSET_COUNT] = 1; // message count
            uint32_t seqNet = htonl(nextSeq_[receiverId - 1]++);
            std::memcpy(buf + OFFSET_SEQ, &seqNet, sizeof(seqNet));
            uint32_t lenNet = htonl(static_cast<uint32_t>(len));
            std::memcpy(buf + OFFSET_LEN, &lenNet, sizeof(lenNet));
            std::memcpy(buf + HEADER_SIZE, data, len);
            socket_.SendPacket(buf, HEADER_SIZE+len, peers_[receiverId - 1]);

        }

        void start(){
            receiverThread_ = std::thread(&PerfectLinks::ReceiveLoop, this);
            receiverThread_.detach();
        }

        unsigned long id() const {
            return id_;
        }


    private:
        SocketWrapper socket_;
        DeliverCallback deliver_;
        unsigned long id_;
        std::vector<sockaddr_in> peers_;
        std::vector<uint32_t> nextSeq_;
        std::thread receiverThread_;

        void DeliverMessage(uint32_t senderId, const uint8_t* data, size_t len) {
            deliver_(senderId, data, len);
        }

        void ReceiveLoop() {
            uint8_t buf[MAX_PACKET_SIZE];
            sockaddr_in from;
            while (true) {
                ssize_t bytesReceived = socket_.ReceivePacket(buf, sizeof(buf), from);
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

                    DeliverMessage(senderId, buf + HEADER_SIZE, len);
                }
            }
        }
};