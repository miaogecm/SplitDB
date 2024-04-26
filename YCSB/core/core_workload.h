//
//  core_workload.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CORE_WORKLOAD_H_
#define YCSB_C_CORE_WORKLOAD_H_

#include "acknowledged_counter_generator.h"
#include "counter_generator.h"
#include "db.h"
#include "discrete_generator.h"
#include "generator.h"
#include "properties.h"
#include "utils.h"
#include <string>
#include <vector>

namespace ycsbc {

  enum Operation {
    INSERT,
    READ,
    UPDATE,
    SCAN,
    READMODIFYWRITE,
    DELETE,
    MAXOPTYPE
  };

  extern const char *kOperationString[MAXOPTYPE];

  class CoreWorkload {
   public:
    ///
    /// The name of the database table to run queries against.
    ///
    static const std::string TABLENAME_PROPERTY;
    static const std::string TABLENAME_DEFAULT;

    ///
    /// The name of the property for the number of fields in a record.
    ///
    static const std::string FIELD_COUNT_PROPERTY;
    static const std::string FIELD_COUNT_DEFAULT;

    ///
    /// The name of the property for the field length distribution.
    /// Options are "uniform", "zipfian" (favoring short records), and
    /// "constant".
    ///
    static const std::string FIELD_LENGTH_DISTRIBUTION_PROPERTY;
    static const std::string FIELD_LENGTH_DISTRIBUTION_DEFAULT;

    ///
    /// The name of the property for the length of a field in bytes.
    ///
    static const std::string FIELD_LENGTH_PROPERTY;
    static const std::string FIELD_LENGTH_DEFAULT;

    ///
    /// The name of the property for deciding whether to read one field (false)
    /// or all fields (true) of a record.
    ///
    static const std::string READ_ALL_FIELDS_PROPERTY;
    static const std::string READ_ALL_FIELDS_DEFAULT;

    ///
    /// The name of the property for deciding whether to write one field (false)
    /// or all fields (true) of a record.
    ///
    static const std::string WRITE_ALL_FIELDS_PROPERTY;
    static const std::string WRITE_ALL_FIELDS_DEFAULT;

    ///
    /// The name of the property for the proportion of read transactions.
    ///
    static const std::string READ_PROPORTION_PROPERTY;
    static const std::string READ_PROPORTION_DEFAULT;

    ///
    /// The name of the property for the proportion of update transactions.
    ///
    static const std::string UPDATE_PROPORTION_PROPERTY;
    static const std::string UPDATE_PROPORTION_DEFAULT;

    ///
    /// The name of the property for the proportion of insert transactions.
    ///
    static const std::string INSERT_PROPORTION_PROPERTY;
    static const std::string INSERT_PROPORTION_DEFAULT;

    ///
    /// The name of the property for the proportion of scan transactions.
    ///
    static const std::string SCAN_PROPORTION_PROPERTY;
    static const std::string SCAN_PROPORTION_DEFAULT;

    ///
    /// The name of the property for the proportion of
    /// read-modify-write transactions.
    ///
    static const std::string READMODIFYWRITE_PROPORTION_PROPERTY;
    static const std::string READMODIFYWRITE_PROPORTION_DEFAULT;

    ///
    /// The name of the property for the the distribution of request keys.
    /// Options are "uniform", "zipfian" and "latest".
    ///
    static const std::string REQUEST_DISTRIBUTION_PROPERTY;
    static const std::string REQUEST_DISTRIBUTION_DEFAULT;

    ///
    /// The default zero padding value. Matches integer sort order
    ///
    static const std::string ZERO_PADDING_PROPERTY;
    static const std::string ZERO_PADDING_DEFAULT;

    ///
    /// The name of the property for the min scan length (number of records).
    ///
    static const std::string MIN_SCAN_LENGTH_PROPERTY;
    static const std::string MIN_SCAN_LENGTH_DEFAULT;

    ///
    /// The name of the property for the max scan length (number of records).
    ///
    static const std::string MAX_SCAN_LENGTH_PROPERTY;
    static const std::string MAX_SCAN_LENGTH_DEFAULT;

    ///
    /// The name of the property for the scan length distribution.
    /// Options are "uniform" and "zipfian" (favoring short scans).
    ///
    static const std::string SCAN_LENGTH_DISTRIBUTION_PROPERTY;
    static const std::string SCAN_LENGTH_DISTRIBUTION_DEFAULT;

    ///
    /// The name of the property for the order to insert records.
    /// Options are "ordered" or "hashed".
    ///
    static const std::string INSERT_ORDER_PROPERTY;
    static const std::string INSERT_ORDER_DEFAULT;

    static const std::string INSERT_START_PROPERTY;
    static const std::string INSERT_START_DEFAULT;

    static const std::string RECORD_COUNT_PROPERTY;
    static const std::string OPERATION_COUNT_PROPERTY;

    ///
    /// Field name prefix.
    ///
    static const std::string FIELD_NAME_PREFIX;
    static const std::string FIELD_NAME_PREFIX_DEFAULT;

    ///
    /// Initialize the scenario.
    /// Called once, in the main client thread, before any operations are
    /// started.
    ///
    virtual void Init(const utils::Properties &p);

    virtual bool DoInsert(DB &db);
    virtual bool DoTransaction(DB &db);

    [[nodiscard]] auto ReadAllFields() const -> bool { return readAllFields; }
    [[nodiscard]] auto WriteAllFields() const -> bool { return writeAllFields; }

    CoreWorkload() = default;

    virtual ~CoreWorkload() {
      delete fieldLenGenerator;
      delete keyChooser;
      delete fieldChooser;
      delete scanLenChooser;
      delete insertKeySequence;
      delete transactionInsertKeySequence;
    }

   protected:
    static Generator<uint64_t> *
    GetFieldLenGenerator(const utils::Properties &p);
    std::string BuildKeyName(uint64_t keyNum) const;
    void BuildValues(std::vector<DB::Field> &values);
    void BuildSingleValue(std::vector<DB::Field> &update);

    uint64_t NextTransactionKeyNum();
    std::string NextFieldName();

    int TransactionRead(DB &db);
    int TransactionReadModifyWrite(DB &db);
    int TransactionScan(DB &db);
    int TransactionUpdate(DB &db);
    int TransactionInsert(DB &db);

    std::string tableName;
    int fieldCount{0};
    std::string fieldPrefix;
    bool readAllFields{false};
    bool writeAllFields{false};
    Generator<uint64_t> *fieldLenGenerator{nullptr};
    DiscreteGenerator<Operation> opChooser;
    Generator<uint64_t> *keyChooser{nullptr}; // transaction key gen
    Generator<uint64_t> *fieldChooser{nullptr};
    Generator<uint64_t> *scanLenChooser{nullptr};
    CounterGenerator *insertKeySequence{nullptr}; // load insert key gen
    AcknowledgedCounterGenerator *transactionInsertKeySequence{
      nullptr}; // transaction insert key gen
    bool orderedInserts{true};
    size_t recordCount{0};
    int zeroPadding;
  };

  inline uint64_t CoreWorkload::NextTransactionKeyNum() {
    uint64_t keyNum;
    do {
      keyNum = keyChooser->Next();
    } while(keyNum > transactionInsertKeySequence->Last());
    return keyNum;
  }

  inline auto CoreWorkload::NextFieldName() -> std::string {
    return std::string(fieldPrefix).append(std::to_string(fieldChooser->Next()));
  }

  inline auto CoreWorkload::DoInsert(DB &db) -> bool {
    const std::string key = BuildKeyName(insertKeySequence->Next());
    std::vector<DB::Field> fields;
    fields.reserve(10);
    BuildValues(fields);
    return db.Insert(tableName, key, fields) == DB::kOK;
  }

  inline auto CoreWorkload::DoTransaction(DB &db) -> bool {
    int status = -1;
    switch(opChooser.Next()) {
    case READ: status = TransactionRead(db); break;
    case UPDATE: status = TransactionUpdate(db); break;
    case INSERT: status = TransactionInsert(db); break;
    case SCAN: status = TransactionScan(db); break;
    case READMODIFYWRITE: status = TransactionReadModifyWrite(db); break;
    default: throw utils::Exception("Operation request is not recognized!");
    }
    assert(status >= 0);
    return (status == DB::kOK);
  }

  inline int CoreWorkload::TransactionRead(DB &db) {
    //    return 1;
    uint64_t keyNum = NextTransactionKeyNum();
    const std::string key = BuildKeyName(keyNum);
    std::vector<DB::Field> result;
    if(!ReadAllFields()) {
      std::vector<std::string> fields;
      fields.push_back(NextFieldName());
      return db.Read(tableName, key, &fields, result);
    } else {
      return db.Read(tableName, key, NULL, result);
    }
  }

  inline int CoreWorkload::TransactionReadModifyWrite(DB &db) {
    uint64_t keyNum = NextTransactionKeyNum();
    const std::string key = BuildKeyName(keyNum);
    std::vector<DB::Field> result;

    if(!ReadAllFields()) {
      std::vector<std::string> fields;
      fields.push_back(NextFieldName());
      db.Read(tableName, key, &fields, result);
    } else {
      db.Read(tableName, key, nullptr, result);
    }

    std::vector<DB::Field> values;
    values.reserve(10);
    if(WriteAllFields()) {
      BuildValues(values);
    } else {
      BuildSingleValue(values);
    }
    return db.Update(tableName, key, values);
  }

  inline int CoreWorkload::TransactionScan(DB &db) {
    uint64_t keyNum = NextTransactionKeyNum();
    const std::string key = BuildKeyName(keyNum);
    int len = scanLenChooser->Next();
    std::vector<std::vector<DB::Field>> result;
    if(!ReadAllFields()) {
      std::vector<std::string> fields;
      fields.push_back(NextFieldName());
      return db.Scan(tableName, key, len, &fields, result);
    } else {
      return db.Scan(tableName, key, len, nullptr, result);
    }
  }

  inline auto CoreWorkload::TransactionUpdate(DB &db) -> int {
    //    return 1;
    uint64_t keyNum = NextTransactionKeyNum();
    const std::string key = BuildKeyName(keyNum);
    std::vector<DB::Field> values;
    values.reserve(10);
    if(WriteAllFields()) {
      BuildValues(values);
    } else {
      BuildSingleValue(values);
    }
    return db.Update(tableName, key, values);
    //    return db.Insert(tableName, key, values);
  }

  inline auto CoreWorkload::TransactionInsert(DB &db) -> int {
    uint64_t keyNum = transactionInsertKeySequence->Next();
    const std::string key = BuildKeyName(keyNum);
    std::vector<DB::Field> values;
    values.reserve(10);
    BuildValues(values);
    int s = db.Insert(tableName, key, values);
    transactionInsertKeySequence->Acknowledge(keyNum);
    return s;
  }

} // namespace ycsbc

#endif // YCSB_C_CORE_WORKLOAD_H_
