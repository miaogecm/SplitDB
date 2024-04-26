//

//

#include "db/db_impl.h"
#include "YCSB/core/zipfian_generator.h"
#include <cstdio>
#include <iostream>

auto main() -> int {
  std::map<uint64_t, uint64_t> cnt{};
  auto total = 100000;
  printf("Running main() from %s\n", __FILE__);
  std::cout << "test begin" << std::endl;
  ycsbc::ZipfianGenerator zipfianGenerator(total);
  for(int i = 0; i < total; i++) {
    auto a = zipfianGenerator.Next();

    //    std::string preKey = "user";
    //    std::string value = std::to_string(keyNum);
    //    int fill = std::max(0, zeroPadding - static_cast<int>(value.size()));
    //    preKey.append(fill, '0').append(value);
  }
  for(auto &i : cnt) {
    std::cout << i.first << " " << i.second << std::endl;
  }
}
