#include "broker/broker.hpp"
#include <iostream>

int main() {
  try {
    fell::Broker broker("fell-data");
    broker.run(7700);
  } catch (const std::exception &e) {
    std::cerr << "Fatal broker daemon error: " << e.what() << std::endl;
    return 1;
  }
  return 0;
}
