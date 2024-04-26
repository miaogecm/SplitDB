//
//  core_workload.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "core_workload.h"
#include "const_generator.h"
#include "random_byte_generator.h"
#include "scrambled_zipfian_generator.h"
#include "skewed_latest_generator.h"
#include "uniform_generator.h"
#include "utils.h"
#include "zipfian_generator.h"

#include <algorithm>
#include <string>

using std::string;
using ycsbc::CoreWorkload;

const char *ycsbc::kOperationString[ycsbc::MAXOPTYPE] = {
  "INSERT", "READ", "UPDATE", "SCAN", "READMODIFYWRITE", "DELETE"};

const string CoreWorkload::TABLENAME_PROPERTY = "table";
const string CoreWorkload::TABLENAME_DEFAULT = "usertable";

const string CoreWorkload::FIELD_COUNT_PROPERTY = "fieldcount";
const string CoreWorkload::FIELD_COUNT_DEFAULT = "10";

const string CoreWorkload::FIELD_LENGTH_DISTRIBUTION_PROPERTY =
  "field_len_dist";
const string CoreWorkload::FIELD_LENGTH_DISTRIBUTION_DEFAULT = "constant";

const string CoreWorkload::FIELD_LENGTH_PROPERTY = "fieldlength";
const string CoreWorkload::FIELD_LENGTH_DEFAULT = "100";

const string CoreWorkload::READ_ALL_FIELDS_PROPERTY = "readallfields";
const string CoreWorkload::READ_ALL_FIELDS_DEFAULT = "true";

const string CoreWorkload::WRITE_ALL_FIELDS_PROPERTY = "writeallfields";
const string CoreWorkload::WRITE_ALL_FIELDS_DEFAULT = "false";

const string CoreWorkload::READ_PROPORTION_PROPERTY = "readproportion";
const string CoreWorkload::READ_PROPORTION_DEFAULT = "0.95";

const string CoreWorkload::UPDATE_PROPORTION_PROPERTY = "updateproportion";
const string CoreWorkload::UPDATE_PROPORTION_DEFAULT = "0.05";

const string CoreWorkload::INSERT_PROPORTION_PROPERTY = "insertproportion";
const string CoreWorkload::INSERT_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::SCAN_PROPORTION_PROPERTY = "scanproportion";
const string CoreWorkload::SCAN_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::READMODIFYWRITE_PROPORTION_PROPERTY =
  "readmodifywriteproportion";
const string CoreWorkload::READMODIFYWRITE_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::REQUEST_DISTRIBUTION_PROPERTY =
  "requestdistribution";
const string CoreWorkload::REQUEST_DISTRIBUTION_DEFAULT = "uniform";

const string CoreWorkload::ZERO_PADDING_PROPERTY = "zeropadding";
const string CoreWorkload::ZERO_PADDING_DEFAULT = "1";

const string CoreWorkload::MIN_SCAN_LENGTH_PROPERTY = "minscanlength";
const string CoreWorkload::MIN_SCAN_LENGTH_DEFAULT = "1";

const string CoreWorkload::MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";
const string CoreWorkload::MAX_SCAN_LENGTH_DEFAULT = "1000";

const string CoreWorkload::SCAN_LENGTH_DISTRIBUTION_PROPERTY =
  "scanlengthdistribution";
const string CoreWorkload::SCAN_LENGTH_DISTRIBUTION_DEFAULT = "uniform";

const string CoreWorkload::INSERT_ORDER_PROPERTY = "insertorder";
const string CoreWorkload::INSERT_ORDER_DEFAULT = "hashed";

const string CoreWorkload::INSERT_START_PROPERTY = "insertstart";
const string CoreWorkload::INSERT_START_DEFAULT = "0";

const string CoreWorkload::RECORD_COUNT_PROPERTY = "recordcount";
const string CoreWorkload::OPERATION_COUNT_PROPERTY = "operationcount";

const std::string CoreWorkload::FIELD_NAME_PREFIX = "fieldnameprefix";
const std::string CoreWorkload::FIELD_NAME_PREFIX_DEFAULT = "field";

void CoreWorkload::Init(const utils::Properties &p) {
  tableName = p.GetProperty(TABLENAME_PROPERTY, TABLENAME_DEFAULT);

  fieldCount =
    std::stoi(p.GetProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_DEFAULT));
  fieldPrefix = p.GetProperty(FIELD_NAME_PREFIX, FIELD_NAME_PREFIX_DEFAULT);
  fieldLenGenerator = GetFieldLenGenerator(p);

  double readProportion =
    std::stod(p.GetProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_DEFAULT));
  double updateProportion = std::stod(
    p.GetProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_DEFAULT));
  double insertProportion = std::stod(
    p.GetProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_DEFAULT));
  double scanProportion =
    std::stod(p.GetProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_DEFAULT));
  double readModifyWriteProportion = std::stod(p.GetProperty(
    READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_DEFAULT));

  recordCount = std::stoi(p.GetProperty(RECORD_COUNT_PROPERTY));
  std::string requestDist =
    p.GetProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_DEFAULT);
  int minScanLen =
    std::stoi(p.GetProperty(MIN_SCAN_LENGTH_PROPERTY, MIN_SCAN_LENGTH_DEFAULT));
  int maxScanLen =
    std::stoi(p.GetProperty(MAX_SCAN_LENGTH_PROPERTY, MAX_SCAN_LENGTH_DEFAULT));
  std::string scanLenDist = p.GetProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY,
                                          SCAN_LENGTH_DISTRIBUTION_DEFAULT);
  int insertStart =
    std::stoi(p.GetProperty(INSERT_START_PROPERTY, INSERT_START_DEFAULT));

  zeroPadding =
    std::stoi(p.GetProperty(ZERO_PADDING_PROPERTY, ZERO_PADDING_DEFAULT));

  readAllFields = utils::StrToBool(
    p.GetProperty(READ_ALL_FIELDS_PROPERTY, READ_ALL_FIELDS_DEFAULT));
  writeAllFields = utils::StrToBool(
    p.GetProperty(WRITE_ALL_FIELDS_PROPERTY, WRITE_ALL_FIELDS_DEFAULT));

  if(p.GetProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_DEFAULT) == "hashed") {
    orderedInserts = false;
  } else {
    orderedInserts = true;
  }

  if(readProportion > 0) {
    opChooser.AddValue(READ, readProportion);
  }
  if(updateProportion > 0) {
    opChooser.AddValue(UPDATE, updateProportion);
  }
  if(insertProportion > 0) {
    opChooser.AddValue(INSERT, insertProportion);
  }
  if(scanProportion > 0) {
    opChooser.AddValue(SCAN, scanProportion);
  }
  if(readModifyWriteProportion > 0) {
    opChooser.AddValue(READMODIFYWRITE, readModifyWriteProportion);
  }

  insertKeySequence = new CounterGenerator(insertStart);
  transactionInsertKeySequence = new AcknowledgedCounterGenerator(recordCount);

  if(requestDist == "uniform") {
    keyChooser = new UniformGenerator(0, recordCount - 1);

  } else if(requestDist == "zipfian") {
    // If the number of keys changes, we don't want to change popular keys.
    // So we construct the scrambled zipfian generator with a keyspace
    // that is larger than what exists at the beginning of the test.
    // If the generator picks a key that is not inserted yet, we just ignore it
    // and pick another key.
    int opCount = std::stoi(p.GetProperty(OPERATION_COUNT_PROPERTY));
    int newKeys = (int)(opCount * insertProportion * 2); // a fudge factor
    keyChooser = new ScrambledZipfianGenerator(recordCount + newKeys);

  } else if(requestDist == "latest") {
    keyChooser = new SkewedLatestGenerator(*transactionInsertKeySequence);

  } else {
    throw utils::Exception("Unknown request distribution: " + requestDist);
  }

  fieldChooser = new UniformGenerator(0, fieldCount - 1);

  if(scanLenDist == "uniform") {
    scanLenChooser = new UniformGenerator(minScanLen, maxScanLen);
  } else if(scanLenDist == "zipfian") {
    scanLenChooser = new ZipfianGenerator(minScanLen, maxScanLen);
  } else {
    throw utils::Exception("Distribution not allowed for scan length: " +
                           scanLenDist);
  }
}

auto CoreWorkload::GetFieldLenGenerator(const utils::Properties &p)
  -> ycsbc::Generator<uint64_t> * {
  string fieldLenDist = p.GetProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY,
                                      FIELD_LENGTH_DISTRIBUTION_DEFAULT);
  int fieldLen =
    std::stoi(p.GetProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_DEFAULT));
  if(fieldLenDist == "constant") {
    return new ConstGenerator(fieldLen);
  } else if(fieldLenDist == "uniform") {
    return new UniformGenerator(1, fieldLen);
  } else if(fieldLenDist == "zipfian") {
    return new ZipfianGenerator(1, fieldLen);
  } else {
    throw utils::Exception("Unknown field length distribution: " +
                           fieldLenDist);
  }
}

auto CoreWorkload::BuildKeyName(uint64_t keyNum) const -> std::string {
  if(!orderedInserts) {
    keyNum = utils::Hash(keyNum);
  }
  std::string preKey = "user";
  std::string value = std::to_string(keyNum);
  int fill = std::max(0, zeroPadding - static_cast<int>(value.size()));
  return preKey.append(fill, '0').append(value);
}

void CoreWorkload::BuildValues(std::vector<ycsbc::DB::Field> &values) {
  for(int i = 0; i < fieldCount; ++i) {
    values.emplace_back();
    ycsbc::DB::Field &field = values.back();
    field.name.reserve(fieldPrefix.size() + 2);
    field.name.append(fieldPrefix).append(std::to_string(i));
    uint64_t len = fieldLenGenerator->Next();
    field.value.reserve(len);
    RandomByteGenerator byteGenerator;
    std::generate_n(std::back_inserter(field.value), len,
                    [&]() { return byteGenerator.Next(); });
  }
}

void CoreWorkload::BuildSingleValue(std::vector<ycsbc::DB::Field> &values) {
  values.emplace_back();
  ycsbc::DB::Field &field = values.back();
  field.name.append(NextFieldName());
  uint64_t len = fieldLenGenerator->Next();
  field.value.reserve(len);
  RandomByteGenerator byteGenerator;
  std::generate_n(std::back_inserter(field.value), len,
                  [&]() { return byteGenerator.Next(); });
}
