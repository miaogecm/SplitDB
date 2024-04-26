//
//  zipfian_generator.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef LEVELDB_YCSB_CORE_ZIPFIAN_GENERATOR_H_
#define LEVELDB_YCSB_CORE_ZIPFIAN_GENERATOR_H_

#include <cassert>
#include <cmath>
#include <cstdint>
#include <mutex>

#include "generator.h"
#include "utils.h"

namespace ycsbc {

  class ZipfianGenerator : public Generator<uint64_t> {
   public:
    static constexpr double kZipfianConst = ZIPFIAN_CONST;
    //    static constexpr double kZipfianConst = 0.0;
    static constexpr uint64_t kMaxNumItems = (UINT64_MAX >> 24);

    explicit ZipfianGenerator(uint64_t numItems)
        : ZipfianGenerator(0, numItems - 1) {}

    ZipfianGenerator(uint64_t min, uint64_t max,
                     double zipfianConst = kZipfianConst)
        : ZipfianGenerator(min, max, zipfianConst,
                           Zeta_(max - min + 1, zipfianConst)) {}

    ZipfianGenerator(uint64_t min, uint64_t max, double zipfianConst,
                     double zetaN)
        : items_(max - min + 1), base_(min), theta_(zipfianConst),
          allowCountDecrease_(false) {
      assert(items_ >= 2 && items_ < kMaxNumItems);

      zeta_2_ = Zeta_(2, theta_);

      alpha_ = 1.0 / (1.0 - theta_);
      zeta_n_ = zetaN;
      countForZeta_ = items_;
      eta_ = Eta_();

      Next();
    }

    auto Next(uint64_t numItems) -> uint64_t;

    auto Next() -> uint64_t override { return Next(items_); }

    auto Last() -> uint64_t override;

   private:
    [[nodiscard]] auto Eta_() const -> double {
      return (1 - std::pow(2.0 / (double)items_, 1 - theta_)) /
             (1 - zeta_2_ / zeta_n_);
    }

    ///
    /// Calculate the zeta constant needed for a distribution.
    /// Do this incrementally from the last_num of items to the cur_num.
    /// Use the zipfian constant as theta. Remember the new number of items
    /// so that, if it is changed, we can recompute zeta.
    ///
    static auto
    Zeta_(uint64_t lastNum, // NOLINT(bugprone-easily-swappable-parameters)
          uint64_t curNum, double theta, double lastZeta) -> double {
      double zeta = lastZeta;
      for(uint64_t i = lastNum + 1; i <= curNum; ++i) {
        zeta += 1 / std::pow(i, theta);
      }
      return zeta;
    }

    static auto Zeta_(uint64_t num, double theta) -> double {
      return Zeta_(0, num, theta, 0);
    }

    uint64_t items_;
    uint64_t base_; /// Min number of items to generate

    // Computed parameters for generating the distribution
    double theta_, zeta_n_, eta_, alpha_, zeta_2_;
    uint64_t countForZeta_; /// Number of items used to compute zeta_n
    uint64_t lastValue_{};
    std::mutex mutex_;
    bool allowCountDecrease_;
  };

  inline auto ZipfianGenerator::Next(uint64_t numItems) -> uint64_t {
    assert(numItems >= 2 && numItems < kMaxNumItems);
    if(numItems != countForZeta_) {
      // recompute zeta and eta
      std::lock_guard<std::mutex> lock(mutex_);
      if(numItems > countForZeta_) {
        zeta_n_ = Zeta_(countForZeta_, numItems, theta_, zeta_n_);
        countForZeta_ = numItems;
        eta_ = Eta_();
      } else if(numItems < countForZeta_ && allowCountDecrease_) {
        // TODO
      }
    }

    double u = utils::ThreadLocalRandomDouble();
    double uz = u * zeta_n_;

    if(uz < 1.0)
      return lastValue_ = base_;

    if(uz < 1.0 + std::pow(0.5, theta_))
      return lastValue_ = base_ + 1;

    return lastValue_ = (uint64_t)((double)base_ +
                                   (double)numItems *
                                     std::pow(eta_ * u - eta_ + 1, alpha_));
  }

  inline uint64_t ZipfianGenerator::Last() { return lastValue_; }

} // namespace ycsbc

#endif // LEVELDB_YCSB_CORE_ZIPFIAN_GENERATOR_H_
