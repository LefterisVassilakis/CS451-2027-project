#include <sys/socket.h>
#include <stdio.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <cstring>
#include <iostream>


class SocketWrapper{
    public:

        SocketWrapper(unsigned short port){
            createSocket();
            bindSocket(port);
        }

        int SendMessage(const char *message, const char *ip, short unsigned port){
            struct sockaddr_in dest_addr;
            dest_addr.sin_family = AF_INET;
            dest_addr.sin_port = htons(port);
            dest_addr.sin_addr.s_addr = inet_addr(ip);

            if (sendto(sockfd, message, strlen(message), 0, reinterpret_cast<struct sockaddr*>(&dest_addr), sizeof(dest_addr)) < 0) {
                perror("sendto");
                exit(EXIT_FAILURE);
            }
            return 0;
        }

        ssize_t ReceiveMessage(char *buffer, int bufferSize){
            socklen_t addr_len = sizeof(addr);
            ssize_t bytes_received = recvfrom(sockfd, buffer, bufferSize - 1, 0, reinterpret_cast<struct sockaddr*>(&addr), &addr_len);
            if (bytes_received < 0) {
                perror("recvfrom");
                exit(EXIT_FAILURE);
            }
            buffer[bytes_received] = '\0';
            std::cout << "Received message from " << inet_ntoa(addr.sin_addr) << ":" << ntohs(addr.sin_port) << std::endl;
            return bytes_received;
        }

        ~SocketWrapper(){
            closeSocket();
        }

    private:
        int sockfd;
        struct sockaddr_in addr;

        void createSocket(){
            
            sockfd = socket(AF_INET, SOCK_DGRAM, 0);
            if (sockfd < 0) {
                perror("socket");
                exit(EXIT_FAILURE);
            }
        }

        void bindSocket(unsigned short port){

            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = INADDR_ANY;
            addr.sin_port = htons(port);

            if (bind(sockfd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
                perror("bind");
                exit(EXIT_FAILURE);
            }
        }

        void closeSocket(){
            close(sockfd);
        }
};