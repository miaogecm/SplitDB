//
//  leveldb_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#include "leveldb_db.h"
#include "core/core_workload.h"
#include "core/db_factory.h"
#include "core/properties.h"
#include "core/utils.h"
#include <leveldb/options.h>
#include <leveldb/write_batch.h>
#include <x86intrin.h>

namespace {
  const std::string PROP_NAME = "leveldb.dbname";
  const std::string PROP_NAME_DEFAULT = "";

  const std::string PROP_FORMAT = "leveldb.format";
  const std::string PROP_FORMAT_DEFAULT = "single";

  const std::string PROP_DESTROY = "leveldb.destroy";
  const std::string PROP_DESTROY_DEFAULT = "false";

  const std::string PROP_COMPRESSION = "leveldb.compression";
  const std::string PROP_COMPRESSION_DEFAULT = "no";

  const std::string PROP_WRITE_BUFFER_SIZE = "leveldb.write_buffer_size";
  const std::string PROP_WRITE_BUFFER_SIZE_DEFAULT = "0";

  const std::string PROP_MAX_FILE_SIZE = "leveldb.max_file_size";
  const std::string PROP_MAX_FILE_SIZE_DEFAULT = "0";

  const std::string PROP_MAX_OPEN_FILES = "leveldb.max_open_files";
  const std::string PROP_MAX_OPEN_FILES_DEFAULT = "0";

  const std::string PROP_CACHE_SIZE = "leveldb.cache_size";
  const std::string PROP_CACHE_SIZE_DEFAULT = "0";

  const std::string PROP_FILTER_BITS = "leveldb.filter_bits";
  const std::string PROP_FILTER_BITS_DEFAULT = "0";

  const std::string PROP_BLOCK_SIZE = "leveldb.block_size";
  const std::string PROP_BLOCK_SIZE_DEFAULT = "0";

  const std::string PROP_BLOCK_RESTART_INTERVAL =
    "leveldb.block_restart_interval";
  const std::string PROP_BLOCK_RESTART_INTERVAL_DEFAULT = "0";
} // namespace

namespace ycsbc {

  leveldb::DB *LeveldbDB::db_ = nullptr;
  int LeveldbDB::ref_cnt_ = 0;
  std::mutex LeveldbDB::mu_;

  void LeveldbDB::Init() {
    const std::lock_guard<std::mutex> lock(mu_);

    const utils::Properties &props = *props_;
    const std::string &format =
      props.GetProperty(PROP_FORMAT, PROP_FORMAT_DEFAULT);
    if(format == "single") {
      format_ = kSingleEntry;
      method_read_ = &LeveldbDB::ReadSingleEntry;
      method_scan_ = &LeveldbDB::ScanSingleEntry;
      method_update_ = &LeveldbDB::UpdateSingleEntry;
      method_insert_ = &LeveldbDB::InsertSingleEntry;
      method_delete_ = &LeveldbDB::DeleteSingleEntry;
    } else if(format == "row") {
      format_ = kRowMajor;
      method_read_ = &LeveldbDB::ReadCompKeyRM;
      method_scan_ = &LeveldbDB::ScanCompKeyRM;
      method_update_ = &LeveldbDB::InsertCompKey;
      method_insert_ = &LeveldbDB::InsertCompKey;
      method_delete_ = &LeveldbDB::DeleteCompKey;
    } else if(format == "column") {
      format_ = kColumnMajor;
      method_read_ = &LeveldbDB::ReadCompKeyCM;
      method_scan_ = &LeveldbDB::ScanCompKeyCM;
      method_update_ = &LeveldbDB::InsertCompKey;
      method_insert_ = &LeveldbDB::InsertCompKey;
      method_delete_ = &LeveldbDB::DeleteCompKey;
    } else {
      throw utils::Exception("unknown format");
    }
    fieldcount_ = std::stoi(props.GetProperty(
      CoreWorkload::FIELD_COUNT_PROPERTY, CoreWorkload::FIELD_COUNT_DEFAULT));
    field_prefix_ = props.GetProperty(CoreWorkload::FIELD_NAME_PREFIX,
                                      CoreWorkload::FIELD_NAME_PREFIX_DEFAULT);

    ref_cnt_++;
    if(db_) {
      return;
    }

    const std::string &db_path =
      props.GetProperty(PROP_NAME, PROP_NAME_DEFAULT);
    if(db_path == "") {
      throw utils::Exception("LevelDB db path is missing");
    }

    leveldb::Options opt;
    opt.create_if_missing = true;
    GetOptions_(props, &opt);

    leveldb::Status s;

    if(props.GetProperty(PROP_DESTROY, PROP_DESTROY_DEFAULT) == "true") {
      s = leveldb::DestroyDb(db_path, opt);
      if(!s.ok()) {
        throw utils::Exception(std::string("LevelDB DestroyDB: ") +
                               s.ToString());
      }
    }
    s = leveldb::DB::Open(opt, db_path, &db_);
    if(!s.ok()) {
      throw utils::Exception(std::string("LevelDB Open: ") + s.ToString());
    }
  }

  void LeveldbDB::Cleanup() {
    const std::lock_guard<std::mutex> lock(mu_);
    if(--ref_cnt_) {
      return;
    }

    //    getchar();
    //    getchar();

    delete db_;
  }

  void LeveldbDB::GetOptions_(const utils::Properties &props,
                              leveldb::Options *opt) {
    size_t writer_buffer_size = std::stol(props.GetProperty(
      PROP_WRITE_BUFFER_SIZE, PROP_WRITE_BUFFER_SIZE_DEFAULT));
    if(writer_buffer_size > 0) {
      opt->write_buffer_size = writer_buffer_size;
    }
    size_t max_file_size = std::stol(
      props.GetProperty(PROP_MAX_FILE_SIZE, PROP_MAX_FILE_SIZE_DEFAULT));
    if(max_file_size > 0) {
      opt->max_file_size = max_file_size;
    }
    size_t cacheSize =
      std::stol(props.GetProperty(PROP_CACHE_SIZE, PROP_CACHE_SIZE_DEFAULT));
    //    if(cacheSize > 0) {
    opt->block_cache = leveldb::NewLRUCache(0);
    //    }
    int maxOpenFiles = std::stoi(
      props.GetProperty(PROP_MAX_OPEN_FILES, PROP_MAX_OPEN_FILES_DEFAULT));
    if(maxOpenFiles > 0) {
      opt->max_open_files = maxOpenFiles;
    }
    std::string compression =
      props.GetProperty(PROP_COMPRESSION, PROP_COMPRESSION_DEFAULT);
    if(compression == "snappy") {
      opt->compression = leveldb::kSnappyCompression;
    } else {
      opt->compression = leveldb::kNoCompression;
    }
    int filter_bits =
      std::stoi(props.GetProperty(PROP_FILTER_BITS, PROP_FILTER_BITS_DEFAULT));
    if(filter_bits > 0) {
      opt->filter_policy = leveldb::NewBloomFilterPolicy(filter_bits);
    }
    int block_size =
      std::stoi(props.GetProperty(PROP_BLOCK_SIZE, PROP_BLOCK_SIZE_DEFAULT));
    if(block_size > 0) {
      opt->block_size = block_size;
    }
    int block_restart_interval = std::stoi(props.GetProperty(
      PROP_BLOCK_RESTART_INTERVAL, PROP_BLOCK_RESTART_INTERVAL_DEFAULT));
    if(block_restart_interval > 0) {
      opt->block_restart_interval = block_restart_interval;
    }
  }

  void LeveldbDB::SerializeRow_(const std::vector<Field> &values,
                                std::string *data) {
    data->reserve(1024);
    for(const Field &field : values) {
      uint32_t len = field.name.size();
      data->append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
      data->append(field.name.data(), field.name.size());
      len = field.value.size();
      data->append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
      data->append(field.value.data(), field.value.size());
    }
  }

  void
  LeveldbDB::DeserializeRowFilter_(std::vector<Field> *values,
                                   const std::string &data,
                                   const std::vector<std::string> &fields) {
    const char *p = data.data();
    const char *lim = p + data.size();

    std::vector<std::string>::const_iterator filter_iter = fields.begin();
    while(p != lim && filter_iter != fields.end()) {
      assert(p < lim);
      uint32_t len = *reinterpret_cast<const uint32_t *>(p);
      p += sizeof(uint32_t);
      std::string field(p, static_cast<const size_t>(len));
      p += len;
      len = *reinterpret_cast<const uint32_t *>(p);
      p += sizeof(uint32_t);
      std::string value(p, static_cast<const size_t>(len));
      p += len;
      if(*filter_iter == field) {
        values->push_back({field, value});
        filter_iter++;
      }
    }
    assert(values->size() == fields.size());
  }

  void LeveldbDB::DeserializeRow(std::vector<Field> *values,
                                 const std::string &data) {
    const char *p = data.data();
    const char *lim = p + data.size();
    while(p != lim) {
      assert(p < lim);
      uint32_t len = *reinterpret_cast<const uint32_t *>(p);
      p += sizeof(uint32_t);
      std::string field(p, static_cast<const size_t>(len));
      p += len;
      len = *reinterpret_cast<const uint32_t *>(p);
      p += sizeof(uint32_t);
      std::string value(p, static_cast<const size_t>(len));
      p += len;
      values->push_back({field, value});
    }
    //    assert(values->size() == (uint64_t)fieldcount_);
  }

  std::string LeveldbDB::BuildCompKey(const std::string &key,
                                      const std::string &field_name) {
    switch(format_) {
    case kRowMajor: return key + ":" + field_name; break;
    case kColumnMajor: return field_name + ":" + key; break;
    default: throw utils::Exception("wrong format");
    }
  }

  std::string LeveldbDB::KeyFromCompKey(const std::string &comp_key) {
    size_t idx = comp_key.find(":");
    assert(idx != std::string::npos);
    return comp_key.substr(0, idx);
  }

  std::string LeveldbDB::FieldFromCompKey(const std::string &comp_key) {
    size_t idx = comp_key.find(":");
    assert(idx != std::string::npos);
    return comp_key.substr(idx + 1);
  }

  DB::Status
  LeveldbDB::ReadSingleEntry(const std::string &table, const std::string &key,
                             const std::vector<std::string> *fields,
                             std::vector<Field> &result) {
    std::string data;
    //    if(key == "user13635281188657047670"){
    //      std::cerr<<"!";
    //    }
    leveldb::Status s =
      db_->Get(leveldb::ReadOptions(), leveldb::Slice{key}, &data);
    //    if(key == "user13635281188657047670"){
    //      std::cerr<<"!";
    //    }
    if(s.IsNotFound()) {
      //      std::cerr << key << std::endl;
      return kNotFound;
    } else if(!s.ok()) {
      //      std::cerr << key << std::endl;
      throw utils::Exception(std::string("LevelDB Get: ") + s.ToString());
    }
    if(fields != nullptr) {
      DeserializeRowFilter_(&result, data, *fields);
    } else {
      DeserializeRow(&result, data);
    }
    return kOK;
  }

  auto
  LeveldbDB::ScanSingleEntry(const std::string &table, const std::string &key,
                             int len, const std::vector<std::string> *fields,
                             std::vector<std::vector<Field>> &result)
    -> DB::Status {
    leveldb::Iterator *dbIter = db_->NewIterator(leveldb::ReadOptions());
    dbIter->Seek(leveldb::Slice{key});
    for(int i = 0; dbIter->Valid() && i < len; i++) {
      std::string data = dbIter->value().ToString();
      result.emplace_back();
      std::vector<Field> &values = result.back();
      if(fields != nullptr) {
        DeserializeRowFilter_(&values, data, *fields);
      } else {
        DeserializeRow(&values, data);
      }
      dbIter->Next();
    }
    delete dbIter;
    return kOK;
  }

  auto
  LeveldbDB::UpdateSingleEntry(const std::string &table, const std::string &key,
                               std::vector<Field> &values) -> DB::Status {
    std::string data;

    leveldb::Status s =
      db_->Get(leveldb::ReadOptions(), leveldb::Slice{key}, &data);
    //    leveldb::Status s{};
    //    data.resize(1000);

    if(s.IsNotFound())
      return kNotFound;
    else if(!s.ok())
      throw utils::Exception(std::string("LevelDB Get: ") + s.ToString());

    std::vector<Field> currentValues;
    DeserializeRow(&currentValues, data);
    for(Field &newField : values) {
      bool found __attribute__((unused)) = false;
      for(Field &curField : currentValues) {
        if(curField.name == newField.name) {
          found = true;
          curField.value = newField.value;
          break;
        }
      }
      //      assert(found);
    }
    leveldb::WriteOptions wopt;

    data.clear();
    SerializeRow_(currentValues, &data);
    s = db_->Put(wopt, leveldb::Slice{key}, leveldb::Slice{data});
    if(!s.ok())
      throw utils::Exception(std::string("LevelDB Put: ") + s.ToString());
    return kOK;
  }

  auto
  LeveldbDB::InsertSingleEntry(const std::string &table, const std::string &key,
                               std::vector<Field> &values) -> DB::Status {
    std::string data;
    SerializeRow_(values, &data);
    leveldb::WriteOptions wopt;
    leveldb::Status s =
      db_->Put(wopt, leveldb::Slice{key}, leveldb::Slice{data});
    if(!s.ok()) {
      throw utils::Exception(std::string("LevelDB Put: ") + s.ToString());
    }
    return kOK;
  }

  auto LeveldbDB::DeleteSingleEntry(const std::string &table,
                                    const std::string &key) -> DB::Status {
    leveldb::WriteOptions wopt;
    leveldb::Status s = db_->Delete(wopt, leveldb::Slice{key});
    if(!s.ok()) {
      throw utils::Exception(std::string("LevelDB Delete: ") + s.ToString());
    }
    return kOK;
  }

  DB::Status
  LeveldbDB::ReadCompKeyRM(const std::string &table, const std::string &key,
                           const std::vector<std::string> *fields,
                           std::vector<Field> &result) {
    leveldb::Iterator *db_iter = db_->NewIterator(leveldb::ReadOptions());
    db_iter->Seek(leveldb::Slice{key});
    if(!db_iter->Valid() || KeyFromCompKey(db_iter->key().ToString()) != key) {
      return kNotFound;
    }
    if(fields != nullptr) {
      std::vector<std::string>::const_iterator filter_iter = fields->begin();
      for(int i = 0;
          i < fieldcount_ && filter_iter != fields->end() && db_iter->Valid();
          i++) {
        std::string comp_key = db_iter->key().ToString();
        std::string cur_val = db_iter->value().ToString();
        std::string cur_key = KeyFromCompKey(comp_key);
        std::string cur_field = FieldFromCompKey(comp_key);
        assert(cur_key == key);
        assert(cur_field == field_prefix_ + std::to_string(i));

        if(cur_field == *filter_iter) {
          result.push_back({cur_field, cur_val});
          filter_iter++;
        }
        db_iter->Next();
      }
      assert(result.size() == fields->size());
    } else {
      for(int i = 0; i < fieldcount_ && db_iter->Valid(); i++) {
        std::string comp_key = db_iter->key().ToString();
        std::string cur_val = db_iter->value().ToString();
        std::string cur_key = KeyFromCompKey(comp_key);
        std::string cur_field = FieldFromCompKey(comp_key);
        assert(cur_key == key);
        assert(cur_field == field_prefix_ + std::to_string(i));

        result.push_back({cur_field, cur_val});
        db_iter->Next();
      }
      assert(result.size() == (uint64_t)fieldcount_);
    }
    delete db_iter;
    return kOK;
  }

  DB::Status
  LeveldbDB::ScanCompKeyRM(const std::string &table, const std::string &key,
                           int len, const std::vector<std::string> *fields,
                           std::vector<std::vector<Field>> &result) {
    leveldb::Iterator *db_iter = db_->NewIterator(leveldb::ReadOptions());
    db_iter->Seek(leveldb::Slice{key});
    assert(db_iter->Valid() &&
           KeyFromCompKey(db_iter->key().ToString()) == key);
    for(int i = 0; i < len && db_iter->Valid(); i++) {
      result.push_back(std::vector<Field>());
      std::vector<Field> &values = result.back();
      if(fields != nullptr) {
        std::vector<std::string>::const_iterator filter_iter = fields->begin();
        for(int j = 0;
            j < fieldcount_ && filter_iter != fields->end() && db_iter->Valid();
            j++) {
          std::string comp_key = db_iter->key().ToString();
          std::string cur_val = db_iter->value().ToString();
          std::string cur_key = KeyFromCompKey(comp_key);
          std::string cur_field = FieldFromCompKey(comp_key);
          assert(cur_field == field_prefix_ + std::to_string(j));

          if(cur_field == *filter_iter) {
            values.push_back({cur_field, cur_val});
            filter_iter++;
          }
          db_iter->Next();
        }
        assert(values.size() == fields->size());
      } else {
        for(int j = 0; j < fieldcount_ && db_iter->Valid(); j++) {
          std::string comp_key = db_iter->key().ToString();
          std::string cur_val = db_iter->value().ToString();
          std::string cur_key = KeyFromCompKey(comp_key);
          std::string cur_field = FieldFromCompKey(comp_key);
          assert(cur_field == field_prefix_ + std::to_string(j));

          values.push_back({cur_field, cur_val});
          db_iter->Next();
        }
        assert(values.size() == (uint64_t)fieldcount_);
      }
    }
    delete db_iter;
    return kOK;
  }

  DB::Status
  LeveldbDB::ReadCompKeyCM(const std::string &table, const std::string &key,
                           const std::vector<std::string> *fields,
                           std::vector<Field> &result) {
    return kNotImplemented;
  }

  DB::Status
  LeveldbDB::ScanCompKeyCM(const std::string &table, const std::string &key,
                           int len, const std::vector<std::string> *fields,
                           std::vector<std::vector<Field>> &result) {
    return kNotImplemented;
  }

  DB::Status
  LeveldbDB::InsertCompKey(const std::string &table, const std::string &key,
                           std::vector<Field> &values) {
    leveldb::WriteOptions wopt;
    leveldb::WriteBatch batch;

    std::string comp_key;
    for(Field &field : values) {
      comp_key = BuildCompKey(key, field.name);
      batch.Put(leveldb::Slice{comp_key}, leveldb::Slice{field.value});
    }

    leveldb::Status s = db_->Write(wopt, &batch);
    if(!s.ok()) {
      throw utils::Exception(std::string("LevelDB Write: ") + s.ToString());
    }
    return kOK;
  }

  DB::Status
  LeveldbDB::DeleteCompKey(const std::string &table, const std::string &key) {
    leveldb::WriteOptions wopt;
    leveldb::WriteBatch batch;

    std::string comp_key;
    for(int i = 0; i < fieldcount_; i++) {
      comp_key = BuildCompKey(key, field_prefix_ + std::to_string(i));
      batch.Delete(leveldb::Slice{comp_key});
    }

    leveldb::Status s = db_->Write(wopt, &batch);
    if(!s.ok()) {
      throw utils::Exception(std::string("LevelDB Write: ") + s.ToString());
    }
    return kOK;
  }

  //  thread_local int readCnt = 1;
  //
  // #define TSC_KHZ 2194843
  // #define TSC2NS(tsc) (((tsc)*1000000) / TSC_KHZ)

  DB::Status LeveldbDB::Read(const std::string &table, const std::string &key,
                             const std::vector<std::string> *fields,
                             std::vector<Field> &result) {
    //    bool getTime = false;
    //    if(readCnt++ % 10 == 0) {
    //      readCnt = 1;
    //      getTime = true;
    //    }
    //
    //    unsigned int dummy;
    //    uint64_t start, end;
    //    if(getTime) {
    //      start = __rdtscp(&dummy);
    //    }

    auto ret = (this->*(method_read_))(table, key, fields, result);

    //    if(getTime) {
    //      end = __rdtscp(&dummy);
    //
    //      std::stringstream ss;
    //      ss << "readDelayTime = " << TSC2NS(end - start) << std::endl;
    //      std::cout << ss.str() << std::flush;
    //    }

    return ret;
  }

  auto NewLeveldbDB() -> DB * { return new LeveldbDB; }

  const bool registered = DBFactory::RegisterDB("leveldb", NewLeveldbDB);

} // namespace ycsbc
