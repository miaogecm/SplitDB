//
//  basic_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "db_factory.h"
#include "basic_db.h"
#include "db_wrapper.h"

namespace ycsbc {

  std::map<std::string, DBFactory::DBCreator> &DBFactory::Registry() {
    static std::map<std::string, DBCreator> registry;
    return registry;
  }

  bool DBFactory::RegisterDB(std::string db_name, DBCreator db_creator) {
    Registry()[db_name] = db_creator;
    return true;
  }

  auto DBFactory::CreateDB(utils::Properties *props, Measurements *measurements)
    -> DB * {
    std::string dbName = props->GetProperty("dbname", "basic");
    DB *db = nullptr;
    std::map<std::string, DBCreator> &registry = Registry();
    if(registry.find(dbName) != registry.end()) {
      DB *newDb = (*registry[dbName])();
      newDb->SetProps(props);
      db = new DbWrapper(newDb, measurements);
    }
    return db;
  }

} // namespace ycsbc
