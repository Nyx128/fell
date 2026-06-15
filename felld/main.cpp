#include "broker/broker.hpp"
#include "platform/socket.hpp"
#include <iostream>
#include <string>

int main(int argc, char *argv[]) {
  try {
    fell::platform::platform_net_init();
    std::string config_path = "";
    for (int i = 1; i < argc; ++i) {
      std::string arg = argv[i];
      if (arg == "--config" && i + 1 < argc) {
        config_path = argv[++i];
      }
    }

    fell::repl::ClusterConfig cfg = fell::repl::ClusterConfig::load(config_path);

    uint16_t port = cfg.client_port;
    fell::Broker broker(std::move(cfg));
    broker.run(port);
  } catch (const std::exception &e) {
    std::cerr << "Fatal broker daemon error: " << e.what() << std::endl;
    return 1;
  }
  return 0;
}
