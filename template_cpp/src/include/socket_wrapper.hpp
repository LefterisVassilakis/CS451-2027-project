#include <sys/socket.h>
#include <stdio.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>

class SocketWrapper{
    public:

        explicit SocketWrapper(unsigned short port){
            createSocket();
            bindSocket(port);
        }

        int SendMessage(const void *data, size_t len, const sockaddr_in& dest_addr){
            if (sendto(sockfd, data, len, 0, reinterpret_cast<const struct sockaddr*>(&dest_addr), sizeof(dest_addr)) < 0) {
                perror("sendto");
                return -1;
            }
            return 0;
        }

        ssize_t ReceiveMessage(char *buffer, size_t buffer_size, sockaddr_in& from){
            socklen_t addr_len = sizeof(from);
            ssize_t bytes_received = recvfrom(sockfd, buffer, buffer_size, 0, reinterpret_cast<struct sockaddr*>(&from), &addr_len);
            if (bytes_received < 0) {
                perror("recvfrom");
                return -1;
            }
            return bytes_received;
        }

        ~SocketWrapper(){
            closeSocket();
        }

        SocketWrapper(const SocketWrapper&) = delete;
        SocketWrapper& operator=(const SocketWrapper&) = delete;

    private:
        int sockfd;

        void createSocket(){
            sockfd = socket(AF_INET, SOCK_DGRAM, 0);
            if (sockfd < 0) {
                perror("socket");
                exit(EXIT_FAILURE);
            }
        }

        void bindSocket(unsigned short port){
            struct sockaddr_in addr;

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