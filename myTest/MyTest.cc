////
//// Created by jxz on 22-7-7.
////
// #include "bits/stdc++.h"
// #include "pmCache/dbWithPmCache/help/pmem.h"
//
// using namespace std;
//
// #include <libpmemobj++/make_persistent.hpp>
// #include <libpmemobj++/persistent_ptr.hpp>
// #include <libpmemobj++/pext.hpp>
// #include <libpmemobj++/pool.hpp>
// #include <libpmemobj++/transaction.hpp>
//
// using namespace pmem::obj;
//
// template <typename T> struct simple_ptr {
//   simple_ptr() {
//     assert(pmemobj_tx_stage() == TX_STAGE_WORK);
//     ptr = make_persistent<T>();
//   }
//
//   ~simple_ptr() {
//     assert(pmemobj_tx_stage() == TX_STAGE_WORK);
//
//     try {
//       delete_persistent<T>(ptr);
//     }
//     catch(pmem::transaction_free_error &e) {
//       std::cerr << e.what() << std::endl;
//       std::terminate();
//     }
//     catch(pmem::transaction_scope_error &e) {
//       std::cerr << e.what() << std::endl;
//       std::terminate();
//     }
//   }
//
//   persistent_ptr<T> ptr;
// };
//
// struct A {
//   A() : ptr1(), ptr2() {}
//
//   simple_ptr<int> ptr1;
//   simple_ptr<char[(1ULL << 30)]> ptr2;
// };
//
// struct B {
//   B() : ptr1(), ptr2() {
//     auto pop = pool_base(pmemobj_pool_by_ptr(this));
//
//     // It would result in a crash!
//     // basic_transaction::run(pop, [&]{ throw
//     // std::runtime_error("Error"); });
//
//     flat_transaction::run(pop, [&] { throw std::runtime_error("Error"); });
//   }
//
//   simple_ptr<int> ptr1;
//   simple_ptr<int> ptr2;
// };
//
// void tx_nested_struct_example() {
//   /* pool root structure */
//   struct root {
//     persistent_ptr<A> ptrA;
//     persistent_ptr<B> ptrB;
//   };
//
//   /* create a pmemobj pool */
//   auto pop =
//     pool<root>::create("/home/jxz/pmem0/testPool", "layout",
//     PMEMOBJ_MIN_POOL);
//   auto proot = pop.root();
//
//   auto create_a = [&] { proot->ptrA = make_persistent<A>(); };
//   auto create_b = [&] { proot->ptrB = make_persistent<B>(); };
//
//   try {
//     // It would result in a crash!
//     // basic_transaction::run(pop, create_a);
//
//     flat_transaction::run(pop, create_a);
//
//     /* To see why flat_transaction is necessary let's
//      * consider what happens when calling A ctor. The call stack
//      * will look like this:
//      *
//      *  | ptr2 ctor |
//      *  |-----------|
//      *  | ptr1 ctor |
//      *  |-----------|
//      *  |  A ctor   |
//      *
//      * Since ptr2 is a pointer to some huge array of elements,
//      * calling ptr2 ctor will most likely result in make_persistent
//      * throwing an exception (due to out of memory). This exception
//      * will, in turn, cause stack unwinding - already constructed
//      * elements must be destroyed (in this example ptr1 destructor
//      * will be called).
//      *
//      * If we'd use basic_transaction the allocation failure, apart
//      * from throwing an exception, would also cause the transaction
//      * to abort (by default, in basic_transaction, all transactional
//      * functions failures cause tx abort). This is problematic since
//      * the ptr1 destructor, which is called during stack unwinding,
//      * expects the transaction to be in WORK stage (and the actual
//      * stage is ABORTED). As a result the application will fail on
//      * assert (and probably crash in NDEBUG mode).
//      *
//      * Now, consider what will happen if we'd use flat_transaction
//      * instead. In this case, make_persistent failure will not abort
//      * the transaction, it will only result in an exception. This
//      * means that the transaction is still in WORK stage during
//      * stack unwinding. Only after it completes, the transaction is
//      * aborted (it's happening at the outermost level, when exiting
//      * create_a lambda).
//      */
//   }
//   catch(std::runtime_error &) {
//   }
//
//   try {
//     basic_transaction::run(pop, create_b);
//     flat_transaction::run(pop, create_b);
//
//     /* Running create_b can be done both within basic and flat
//      * transaction. However, note that the transaction used in the B
//      * constructor MUST be a flat_transaction. This is because
//      * flat_transaction does not abort immediately when catching an
//      * exception. Instead it passes it to the outermost transaction
//      * - the abort is performed at that outermost level. In case of
//      * a basic_transaction the abort would be done within the B ctor
//      * and it would result in the same problems as with the previous
//      * example.
//      */
//   }
//   catch(std::runtime_error &) {
//   }
// }
//
// int main() {
//   tx_nested_struct_example();
//   return 0;
// }