// Copyright (c) 2018 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef LEVELDB_UTIL_NO_DESTRUCTOR_H_
#define LEVELDB_UTIL_NO_DESTRUCTOR_H_

#include <type_traits>
#include <utility>

namespace leveldb {

  // Wraps an instance whose destructor is never called.
  //
  // This is intended for use with function-level static variables.
  template <typename InstanceType> class NoDestructor {
   public:
    template <typename... ConstructorArgTypes>
    explicit NoDestructor(ConstructorArgTypes &&...constructorArgs) {
      static_assert(
        sizeof(instanceStorage_) >= sizeof(InstanceType),
        "instance_storage_ is not large enough to hold the instance");
      static_assert(
        alignof(decltype(instanceStorage_)) >= alignof(InstanceType),
        "instance_storage_ does not meet the instance's alignment requirement");
      new(&instanceStorage_)
        InstanceType(std::forward<ConstructorArgTypes>(constructorArgs)...);
    }

    ~NoDestructor() = default;

    NoDestructor(const NoDestructor &) = delete;

    NoDestructor &operator=(const NoDestructor &) = delete;

    auto get() -> InstanceType * {
      return reinterpret_cast<InstanceType *>(&instanceStorage_);
    }

   private:
    typename std::aligned_storage<sizeof(InstanceType),
                                  alignof(InstanceType)>::type instanceStorage_;
  };

} // namespace leveldb

#endif // LEVELDB_UTIL_NO_DESTRUCTOR_H_
