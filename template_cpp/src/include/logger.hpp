#pragma once

#include <string>
#include <mutex>
#include <cstdint>
#include <charconv>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <unistd.h>

static constexpr size_t FLUSH_THRESHOLD = 1 << 20; // 1 MB

class Logger {
    public:
        explicit Logger(const std::string& path) : fd_(open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, 0644)) {
            if (fd_ < 0) {
                perror("open");
                exit(EXIT_FAILURE);
            }

            buffer_.reserve(FLUSH_THRESHOLD + 32);
        }

        ~Logger() {
            close(fd_);
        }

        void LogBroadcast(uint32_t seq) {
            char line[32];
            char* p = line;
            *p++ = 'b';
            *p++ = ' ';
            auto [ptr, ec] = std::to_chars(p, line + sizeof(line), seq);
            if (ec != std::errc()) return;
            p = ptr;

            AppendLine(line, p-line);
        }

        void LogDeliver(uint32_t senderId, uint32_t seq) {
            char line[32];
            char* p = line;
            *p++ = 'd';
            *p++ = ' ';
            auto [ptr, ec] = std::to_chars(p, line + sizeof(line), senderId);
            if (ec != std::errc()) return;
            p = ptr;

            *p++ = ' ';
            auto [ptr2, ec2] = std::to_chars(p, line + sizeof(line), seq);
            if (ec != std::errc()) return;
            p = ptr2;

            AppendLine(line, p-line);
            
        }

        void FlushAll() {
            std::lock_guard<std::mutex> lock(mutex_);
            if (closed_) {
                return;
            }
            WriteBufferLocked();
            closed_ = true;
        }


        Logger(const Logger&) = delete;
        Logger& operator=(const Logger&) = delete;
        
    private:
        int fd_;
        std::string buffer_;
        std::mutex mutex_;
        bool closed_ = false;

        void WriteBufferLocked() {
            const char* p = buffer_.data();
            size_t remaining = buffer_.size();

            while (remaining > 0) {
                ssize_t written = write(fd_, p, remaining);
                if (written < 0) {
                    if (errno == EINTR) {
                        continue;
                    } else {
                        break;
                    }
                }
                remaining -= written;
                p += written;
            }
            buffer_.clear();
        }

        void AppendLine(const char* data, size_t len){
            std::lock_guard<std::mutex> lock(mutex_);

            if (closed_) return;

            buffer_.append(data, len);
            buffer_.push_back('\n');
            
            if (buffer_.size() >= FLUSH_THRESHOLD) {
                WriteBufferLocked();
            }
        }

};