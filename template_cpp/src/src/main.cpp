#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

#include "parser.hpp" //change to inclune PL instead
#include "hello.h"
#include <signal.h>
#include "perfect_links.hpp"


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

  auto config = parser.config();

  std::cout << "Broadcasting and delivering messages...\n\n";
  PerfectLinks pl = PerfectLinks(parser.id(), hosts, [](uint32_t senderId, const uint8_t* data, size_t len) {
    if (len != sizeof(uint32_t)) {
      return;
    }
    uint32_t payloadNet;
    std::memcpy(&payloadNet, data, sizeof(payloadNet));
    uint32_t payload = ntohl(payloadNet);
    std::cout << "Received message from sender ID: " << senderId << ", length: " << len << ", payload: " << payload << "\n";
  });

  if (parser.id() == config.receiver_id) {
    pl.Start();
  } else {
    uint32_t payload;
    std::cout << "Sending " << static_cast<int>(config.messages_num) << " messages to receiver ID: " << config.receiver_id << "\n";
    for (int i = 1; i <= config.messages_num; ++i) {
      payload = htonl(i);
      pl.SendMessage(config.receiver_id, reinterpret_cast<const uint8_t*>(&payload), sizeof(payload));
    }
  }


  // After a process finishes broadcasting,
  // it waits forever for the delivery of messages.
  while (true) {
    std::this_thread::sleep_for(std::chrono::hours(1));
  }

  return 0;
}
