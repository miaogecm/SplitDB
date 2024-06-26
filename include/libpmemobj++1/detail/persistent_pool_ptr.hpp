// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2018-2021, Intel Corporation */

/**
 * @file
 * Persistent pointer for pool handle.
 */

#ifndef PMEMOBJ_PERSISTENT_POOL_PTR_HPP
#define PMEMOBJ_PERSISTENT_POOL_PTR_HPP

#include <cassert>
#include <cstddef>
#include <type_traits>

#include <libpmemobj++/detail/specialization.hpp>
#include <libpmemobj++/persistent_ptr.hpp>

namespace pmem {
  namespace detail {

    template <typename T> class persistent_pool_ptr {
      template <typename Y> friend class persistent_pool_ptr;

      typedef persistent_pool_ptr<T> this_type;

     public:
      /**
       * Type of an actual object with all qualifier removed,
       * used for easy underlying type access
       */
      typedef typename pmem::detail::sp_element<T>::type element_type;

      persistent_pool_ptr() : off(0) { verify_type(); }

      /**
       *  Default null constructor, zeroes the off.
       */
      persistent_pool_ptr(std::nullptr_t) noexcept : off(0) { verify_type(); }

      /**
       * PMEMoid constructor.
       *
       * Provided for easy interoperability between C++ and C API's.
       *
       * @param oid C-style persistent pointer
       */
      persistent_pool_ptr(PMEMoid oid) noexcept : off(oid.off) {
        verify_type();
      }

      /**
       * PMEMoid constructor.
       *
       * Provided for easy interoperability between C++ and C API's.
       *
       * @param off Offset inside persistent memory pool
       */
      persistent_pool_ptr(uint64_t _off) noexcept : off(_off) { verify_type(); }

      /**
       * Copy constructor from a different persistent_pool_ptr<>.
       *
       * Available only for convertible types.
       *
       */
      template <typename Y, typename = typename std::enable_if<
                              std::is_convertible<Y *, T *>::value>::type>
      persistent_pool_ptr(const persistent_pool_ptr<Y> &r) noexcept
          : off(r.off) {
        verify_type();
      }

      /**
       * Copy constructor from a different persistent_ptr<>.
       *
       * Available only for convertible types.
       *
       */
      template <typename Y, typename = typename std::enable_if<
                              std::is_convertible<Y *, T *>::value>::type>
      persistent_pool_ptr(const pmem::obj::persistent_ptr<Y> &r) noexcept
          : off(r.raw().off) {
        verify_type();
      }

      /*
       * Copy constructor.
       *
       * @param r Persistent pool pointer to the same type.
       */
      persistent_pool_ptr(const persistent_pool_ptr &r) noexcept : off(r.off) {
        verify_type();
      }

      /*
       * Copy constructor from a persistent_ptr.
       *
       * @param r Persistent pointer to the same type.
       */
      persistent_pool_ptr(const pmem::obj::persistent_ptr<T> &r) noexcept
          : off(r.raw().off) {
        verify_type();
      }

      /**
       * Move constructor.
       */
      persistent_pool_ptr(persistent_pool_ptr &&r) noexcept
          : off(std::move(r.off)) {
        verify_type();
      }

      /**
       * Move assignment operator.
       */
      persistent_pool_ptr &operator=(persistent_pool_ptr &&r) {
        conditional_add_to_tx(this);
        this->off = std::move(r.off);

        return *this;
      }

      persistent_pool_ptr &operator=(std::nullptr_t) {
        conditional_add_to_tx(this);
        this->off = 0;

        return *this;
      }

      /**
       * Assignment operator.
       *
       * Persistent pool pointer assignment within a transaction
       * automatically registers this operation so that a rollback
       * is possible.
       *
       * @throw pmem::transaction_error when adding the object to the
       *	transaction failed.
       */
      persistent_pool_ptr &operator=(const persistent_pool_ptr &r) {
        conditional_add_to_tx(this);
        this->off = r.off;

        return *this;
      }

      /**
       * Assignment operator from a persistent_ptr.
       *
       * Persistent pool pointer assignment within a transaction
       * automatically registers this operation so that a rollback
       * is possible.
       *
       * @throw pmem::transaction_error when adding the object to the
       *	transaction failed.
       */
      persistent_pool_ptr &operator=(const pmem::obj::persistent_ptr<T> &r) {
        conditional_add_to_tx(this);
        this->off = r.raw().off;

        return *this;
      }

      /**
       * Assignment operator from a PMEMoid.
       *
       * Persistent pool pointer assignment within a transaction
       * automatically registers this operation so that a rollback
       * is possible.
       *
       * @throw pmem::transaction_error when adding the object to the
       *	transaction failed.
       */
      persistent_pool_ptr &operator=(const PMEMoid &oid) {
        conditional_add_to_tx(this);
        this->off = oid.off;
        return *this;
      }

      /**
       * Converting assignment operator from a different
       * persistent_pool_ptr<>.
       *
       * Available only for convertible types.
       * Just like regular assignment, also automatically registers
       * itself in a transaction.
       *
       * @throw pmem::transaction_error when adding the object to the
       *	transaction failed.
       */
      template <typename Y, typename = typename std::enable_if<
                              std::is_convertible<Y *, T *>::value>::type>
      persistent_pool_ptr &operator=(const persistent_pool_ptr<Y> &r) {
        conditional_add_to_tx(this);
        this->off = r.off;

        return *this;
      }

      /**
       * Converting assignment operator from a different
       * persistent_ptr<>.
       *
       * Available only for convertible types.
       * Just like regular assignment, also automatically registers
       * itself in a transaction.
       *
       * @throw pmem::transaction_error when adding the object to the
       *	transaction failed.
       */
      template <typename Y, typename = typename std::enable_if<
                              std::is_convertible<Y *, T *>::value>::type>
      persistent_pool_ptr &operator=(const pmem::obj::persistent_ptr<Y> &r) {
        conditional_add_to_tx(this);
        this->off = r.raw().off;

        return *this;
      }

      /**
       * Get a direct pointer.
       *
       * Performs a calculations on the underlying C-style pointer.
       *
       * @return a direct pointer to the object.
       */
      element_type *get(uint64_t pool_uuid) const noexcept {
        PMEMoid oid = {pool_uuid, this->off};
        return static_cast<element_type *>(pmemobj_direct(oid));
      }

      element_type *operator()(uint64_t pool_uuid) const noexcept {
        return get(pool_uuid);
      }

      /**
       * Get a persistent pointer.
       *
       * Performs a calculations on the underlying C-style pointer.
       *
       * @return a direct pointer to the object.
       */
      pmem::obj::persistent_ptr<T>
      get_persistent_ptr(uint64_t pool_uuid) const noexcept {
        PMEMoid oid = {pool_uuid, this->off};
        return pmem::obj::persistent_ptr<T>(oid);
      }

      /**
       * Swaps two persistent_pool_ptr objects of the same type.
       */
      void swap(persistent_pool_ptr &other) {
        conditional_add_to_tx(this);
        conditional_add_to_tx(&other);
        std::swap(this->off, other.off);
      }

      /*
       * Bool conversion operator.
       */
      explicit operator bool() const noexcept { return this->off != 0; }

      /**
       * Get PMEMoid encapsulated by this object.
       *
       * For C API compatibility.
       *
       * @return const reference to the PMEMoid
       */
      PMEMoid raw_oid(uint64_t pool_uuid) const noexcept {
        PMEMoid oid = {pool_uuid, this->off};
        return oid;
      }

      const uint64_t &raw() const noexcept { return this->off; }

      uint64_t &raw() {
        conditional_add_to_tx(this);
        return this->off;
      }

      /**
       * Prefix increment operator.
       */
      inline persistent_pool_ptr<T> &operator++() {
        conditional_add_to_tx(this);
        this->off += sizeof(T);

        return *this;
      }

      /**
       * Postfix increment operator.
       */
      inline persistent_pool_ptr<T> operator++(int) {
        persistent_pool_ptr<T> ret(*this);
        ++(*this);

        return ret;
      }

      /**
       * Prefix decrement operator.
       */
      inline persistent_pool_ptr<T> &operator--() {
        conditional_add_to_tx(this);
        this->off -= sizeof(T);

        return *this;
      }

      /**
       * Postfix decrement operator.
       */
      inline persistent_pool_ptr<T> operator--(int) {
        persistent_pool_ptr<T> ret(*this);
        --(*this);

        return ret;
      }

      /**
       * Addition assignment operator.
       */
      inline persistent_pool_ptr<T> &operator+=(std::ptrdiff_t s) {
        conditional_add_to_tx(this);
        this->off += s * sizeof(T);

        return *this;
      }

      /**
       * Subtraction assignment operator.
       */
      inline persistent_pool_ptr<T> &operator-=(std::ptrdiff_t s) {
        conditional_add_to_tx(this);
        this->off -= s * sizeof(T);

        return *this;
      }

      inline persistent_pool_ptr<T> operator+(std::ptrdiff_t s) {
        persistent_pool_ptr<T> ret(*this);
        ret.off += s * sizeof(T);

        return ret;
      }

      inline persistent_pool_ptr<T> operator-(std::ptrdiff_t s) {
        persistent_pool_ptr<T> ret(*this);
        ret.off -= s * sizeof(T);

        return ret;
      }

     private:
      /* offset of persistent object in a persistent memory pool */
      uint64_t off;

      void verify_type() {
        static_assert(!std::is_polymorphic<element_type>::value,
                      "Polymorphic types are not supported");
      }
    };

    /**
     * Equality operator.
     *
     * This checks if underlying PMEMoids are equal.
     * @relates persistent_pool_ptr
     */
    template <typename T, typename Y>
    inline bool operator==(const persistent_pool_ptr<T> &lhs,
                           const persistent_pool_ptr<Y> &rhs) noexcept {
      return lhs.raw() == rhs.raw();
    }

    /**
     * Inequality operator.
     * @relates persistent_pool_ptr
     */
    template <typename T, typename Y>
    inline bool operator!=(const persistent_pool_ptr<T> &lhs,
                           const persistent_pool_ptr<Y> &rhs) noexcept {
      return !(lhs == rhs);
    }

    /**
     * Inequality operator with nullptr.
     * @relates persistent_pool_ptr
     */
    template <typename T>
    inline bool
    operator!=(const persistent_pool_ptr<T> &lhs, std::nullptr_t) noexcept {
      return lhs.raw() != 0;
    }

    /**
     * Inequality operator with nullptr.
     * @relates persistent_pool_ptr
     */
    template <typename T>
    inline bool
    operator!=(std::nullptr_t, const persistent_pool_ptr<T> &lhs) noexcept {
      return lhs.raw() != 0;
    }

    /**
     * Equality operator with nullptr.
     * @relates persistent_pool_ptr
     */
    template <typename T>
    inline bool
    operator==(const persistent_pool_ptr<T> &lhs, std::nullptr_t) noexcept {
      return lhs.raw() == 0;
    }

    /**
     * Equality operator with nullptr.
     * @relates persistent_pool_ptr
     */
    template <typename T>
    inline bool
    operator==(std::nullptr_t, const persistent_pool_ptr<T> &lhs) noexcept {
      return lhs.raw() == 0;
    }

    template <class T, class U>
    persistent_pool_ptr<T>
    static_persistent_pool_pointer_cast(const persistent_pool_ptr<U> &r) {
      static_assert(std::is_convertible<T *, U *>::value,
                    "Cannot cast persistent_pool_ptr");
      return persistent_pool_ptr<T>(r.raw());
    }

  } // namespace detail
} // namespace pmem

#endif // PMEMOBJ_PERSISTENT_POOL_PTR_HPP
