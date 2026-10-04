#include <chrono>
#include <iostream>
#include <thread>

#include "parser.hpp"
#include "hello.h"
#include <signal.h>
#include "socket_wrapper.hpp"


static void stop(int) {
  // reset signal handlers to default
  signal(SIGTERM, SIG_DFL);
  signal(SIGINT, SIG_DFL);

  // immediately stop network packet processing
  std::cout << "Immediately stopping network packet processing.\n";

  // write/flush output file if necessary
  std::cout << "Writing output.\n";

  // exit directly from signal handler
  exit(0);
}

int main(int argc, char **argv) {
  signal(SIGTERM, stop);
  signal(SIGINT, stop);

  // `true` means that a config file is required.
  // Call with `false` if no config file is necessary.
  bool requireConfig = true;

  Parser parser(argc, argv);
  parser.parse();

  hello();
  std::cout << std::endl;

  std::cout << "My PID: " << getpid() << "\n";
  std::cout << "From a new terminal type `kill -SIGINT " << getpid() << "` or `kill -SIGTERM "
            << getpid() << "` to stop processing packets\n\n";

  std::cout << "My ID: " << parser.id() << "\n\n";

  std::cout << "List of resolved hosts is:\n";
  std::cout << "==========================\n";
  auto hosts = parser.hosts();
  for (auto &host : hosts) {
    std::cout << host.id << "\n";
    std::cout << "Human-readable IP: " << host.ipReadable() << "\n";
    std::cout << "Machine-readable IP: " << host.ip << "\n";
    std::cout << "Human-readbale Port: " << host.portReadable() << "\n";
    std::cout << "Machine-readbale Port: " << host.port << "\n";
    std::cout << "\n";
  }
  std::cout << "\n";

  std::cout << "Path to output:\n";
  std::cout << "===============\n";
  std::cout << parser.outputPath() << "\n\n";

  std::cout << "Path to config:\n";
  std::cout << "===============\n";
  std::cout << parser.configPath() << "\n\n";

  std::cout << "Doing some initialization...\n\n";

  SocketWrapper socketWrapper = SocketWrapper(hosts[parser.id()].portReadable());
  auto config = parser.config();

  std::cout << "Broadcasting and delivering messages...\n\n";

  if (parser.id() == config.receiver_id) {
    std::cout << "I am the receiver. I will receive " << static_cast<int>(config.messages_num)   << " messages.\n";
    char buffer[1024];
    while (true) {
      socketWrapper.ReceiveMessage(buffer, sizeof(buffer));
      std::cout << "Received message: " << buffer << "\n";
    }
  } else {
    std::cout << "I am a sender. I will send " << static_cast<int>(config.messages_num)  << " messages.\n";
    for (int i = 0; i < config.messages_num; i++) {
      std::string message = "Hello from process " + std::to_string(parser.id())
                            + " message " + std::to_string(i);
      socketWrapper.SendMessage(message.c_str(), hosts[config.receiver_id].ipReadable().c_str(), hosts[config.receiver_id].portReadable());
      std::cout << "Sent message: " << message << "\n";
    }
  }

  // After a process finishes broadcasting,
  // it waits forever for the delivery of messages.
  while (true) {
    std::this_thread::sleep_for(std::chrono::hours(1));
  }

  return 0;
}
