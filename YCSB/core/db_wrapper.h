//
//  db_wrapper.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_DB_WRAPPER_H_
#define YCSB_C_DB_WRAPPER_H_

#include <string>
#include <vector>

#include "db.h"
#include "measurements.h"
#include "timer.h"
#include "utils.h"

namespace ycsbc {

  class DbWrapper : public DB {
   public:
    DbWrapper(DB *db, Measurements *measurements)
        : db_(db), measurements_(measurements) {}
    ~DbWrapper() { delete db_; }
    void Init() override { db_->Init(); }
    void Cleanup() override { db_->Cleanup(); }
    auto Read(const std::string &table, const std::string &key,
              const std::vector<std::string> *fields,
              std::vector<Field> &result) -> Status override {
      timer_.Start();
      Status s = db_->Read(table, key, fields, result);
      uint64_t elapsed = timer_.End();
      measurements_->Report(READ, elapsed);
      return s;
    }
    auto Scan(const std::string &table, const std::string &key, int recordCount,
              const std::vector<std::string> *fields,
              std::vector<std::vector<Field>> &result) -> Status override {
      timer_.Start();
      Status s = db_->Scan(table, key, recordCount, fields, result);
      uint64_t elapsed = timer_.End();
      measurements_->Report(SCAN, elapsed);
      return s;
    }
    auto Update(const std::string &table, const std::string &key,
                std::vector<Field> &values) -> Status override {
      timer_.Start();
      Status s = db_->Update(table, key, values);
      uint64_t elapsed = timer_.End();
      measurements_->Report(UPDATE, elapsed);
      return s;
    }
    Status Insert(const std::string &table, const std::string &key,
                  std::vector<Field> &values) override {
      timer_.Start();
      Status s = db_->Insert(table, key, values);
      uint64_t elapsed = timer_.End();
      measurements_->Report(INSERT, elapsed);
      return s;
    }
    Status Delete(const std::string &table, const std::string &key) override {
      timer_.Start();
      Status s = db_->Delete(table, key);
      uint64_t elapsed = timer_.End();
      measurements_->Report(DELETE, elapsed);
      return s;
    }

   private:
    DB *db_;
    Measurements *measurements_;
    utils::Timer<uint64_t, std::nano> timer_;
  };

} // namespace ycsbc

#endif // YCSB_C_DB_WRAPPER_H_
