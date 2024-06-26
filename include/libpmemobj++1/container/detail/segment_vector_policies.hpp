// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2019, Intel Corporation */

/**
 * @file
 * A persistent version of segment vector implementation.
 */

#ifndef LIBPMEMOBJ_SEGMENT_VECTOR_POLICIES_HPP
#define LIBPMEMOBJ_SEGMENT_VECTOR_POLICIES_HPP

#include <libpmemobj++/container/array.hpp>
#include <libpmemobj++/container/vector.hpp>
#include <libpmemobj++/detail/template_helpers.hpp>
#include <vector>

namespace pmem {
  namespace obj {
    namespace segment_vector_internal {
      template <typename T> using array_64 = array<T, 64>;

      template <typename Container>
      using resize_method =
        decltype(std::declval<Container>().resize(std::declval<size_t>()));

      template <typename Container>
      using container_has_resize = detail::supports<Container, resize_method>;

      template <typename Container,
                bool = container_has_resize<Container>::value>
      struct segment_vector_resize {
        using segment_vector_type = Container;

        static void resize(segment_vector_type &c, size_t n) { c.resize(n); }
      };

      template <typename Container>
      struct segment_vector_resize<Container, false> {
        using segment_vector_type = Container;

        static void resize(segment_vector_type &c, size_t n) {}
      };

      template <template <typename> class SegmentVectorType,
                template <typename> class SegmentType, size_t SegmentSize>
      class fixed_size_policy {
       public:
        /* Traits */
        template <typename T>
        using segment_vector_type = SegmentVectorType<SegmentType<T>>;

        template <typename T> using segment_type = SegmentType<T>;

        template <typename T>
        using value_type = typename segment_type<T>::value_type;

        using size_type = std::size_t;

        template <typename T>
        using segment_vector_resize_type =
          segment_vector_resize<segment_vector_type<T>>;

        static constexpr size_type Size = SegmentSize;

        template <typename T>
        static void resize(segment_vector_type<T> &c, size_type n) {
          segment_vector_resize_type<T>::resize(c, n);
        }

        /**
         * @param[in] index - index of element in segment_vector
         *
         * @return index of segment where element should locate
         */
        static size_type get_segment(size_type index) { return index / Size; }

        /**
         * @param[in] segment_index - index of segment
         *
         * @return index of first element in segment
         */
        static size_type segment_top(size_type segment_index) {
          return segment_index * Size;
        }

        /**
         * @param[in] segment_index - index of segment
         *
         * @return size of segment
         */
        static size_type segment_size(size_type segment_index) { return Size; }

        /**
         * @param[in] index - index of element in segment_vector
         *
         * @return index in segment where it should locate
         */
        static size_type index_in_segment(size_type index) {
          return index % Size;
        }

        /**
         * @return maximum number of elements we can allocate
         */
        template <typename T>
        static size_type max_size(const segment_vector_type<T> &seg_storage) {
          return seg_storage.max_size() * SegmentSize;
        }

        /**
         * @return number of elements in range [0, segment_index]
         */
        static size_type capacity(size_type segment_index) {
          return (segment_index + 1) * Size;
        }
      };

      template <template <typename> class SegmentVectorType,
                template <typename> class SegmentType>
      class exponential_size_policy {
       public:
        /* Traits */
        template <typename T>
        using segment_vector_type = SegmentVectorType<SegmentType<T>>;

        template <typename T> using segment_type = SegmentType<T>;

        template <typename T>
        using value_type = typename segment_type<T>::value_type;

        using size_type = std::size_t;

        template <typename T>
        using segment_vector_resize_type =
          segment_vector_resize<segment_vector_type<T>>;

        template <typename T>
        static void resize(segment_vector_type<T> &c, size_type n) {
          segment_vector_resize_type<T>::resize(c, n);
        }

        /**
         * @param[in] index - index of element in segment_vector
         *
         * @return index of segment where element should locate
         */
        static size_type get_segment(size_type index) {
          return static_cast<size_type>(detail::Log2(index | 1));
        }

        /**
         * @param[in] segment_index - index of segment
         *
         * @return index of first element in segment
         */
        static size_type segment_top(size_type segment_index) {
          return (size_type(1) << segment_index) & ~size_type(1);
        }

        /**
         * @param[in] segment_index - index of segment
         *
         * @return size of segment
         */
        static size_type segment_size(size_type segment_index) {
          return (segment_index == 0) ? 2 : segment_top(segment_index);
        }

        /**
         * @param[in] index - index of element in segment_vector
         *
         * @return index in segment where it should locate
         */
        static size_type index_in_segment(size_type index) {
          return index - segment_top(get_segment(index));
        }

        /**
         * @return maximum number of elements we can allocate
         */
        template <typename T>
        static size_t max_size(const segment_vector_type<T> &) {
          return segment_size(
            get_segment(PMEMOBJ_MAX_ALLOC_SIZE / sizeof(value_type<T>)) + 1);
        }

        /**
         * @return number of elements in range [0, segment_index]
         */
        static size_type capacity(size_type segment_index) {
          if(segment_index == 0)
            return 2;
          return segment_size(segment_index) * 2;
        }
      };

    } // namespace segment_vector_internal
  }   /* namespace obj */
} /* namespace pmem */

#endif /* LIBPMEMOBJ_SEGMENT_VECTOR_POLICIES_HPP */
