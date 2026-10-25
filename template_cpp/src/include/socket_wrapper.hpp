#pragma once

#include <sys/socket.h>
#include <stdio.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>

class SocketWrapper{
    public:

        explicit SocketWrapper(unsigned short port){
            CreateSocket();
            BindSocket(port);
        }

        int SendPacket(const void *data, size_t len, const sockaddr_in& destAddr){
            if (sendto(sockfd_, data, len, 0, reinterpret_cast<const struct sockaddr*>(&destAddr), sizeof(destAddr)) < 0) {
                perror("sendto");
                return -1;
            }
            return 0;
        }

        ssize_t ReceivePacket(void *buffer, size_t bufferSize, sockaddr_in& from){
            socklen_t addr_len = sizeof(from);
            ssize_t bytesReceived = recvfrom(sockfd_, buffer, bufferSize, 0, reinterpret_cast<struct sockaddr*>(&from), &addr_len);
            if (bytesReceived < 0) {
                perror("recvfrom");
                return -1;
            }
            return bytesReceived;
        }

        ~SocketWrapper(){
            CloseSocket();
        }

        SocketWrapper(const SocketWrapper&) = delete;
        SocketWrapper& operator=(const SocketWrapper&) = delete;

    private:
        int sockfd_;

        void CreateSocket(){
            sockfd_ = socket(AF_INET, SOCK_DGRAM, 0);
            if (sockfd_ < 0) {
                perror("socket");
                exit(EXIT_FAILURE);
            }
        }

        void BindSocket(unsigned short port){
            struct sockaddr_in addr{};

            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = INADDR_ANY;
            addr.sin_port = htons(port);

            if (bind(sockfd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
                perror("bind");
                exit(EXIT_FAILURE);
            }
        }

        void CloseSocket(){
            close(sockfd_);
        }
};