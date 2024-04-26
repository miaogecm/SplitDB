////

////
//
//#include "nv_cache_promotion.h"
//#include "db/db_impl.h"
//
//namespace leveldb {
//
//  struct DBImpl::PromotionState {
//   
//    struct Output {
//      uint64_t number{};
//      uint64_t fileSize{};
//      InternalKey smallest, largest;
//      pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNode *pmFile = nullptr;
//    };
//
//    explicit PromotionState(pmCache::Promotion *p) : promotion(p) {}
//
//    pmCache::Promotion *const promotion; 
//
//    std::unique_ptr<WritableFile> outfile{nullptr};
//    std::unique_ptr<TableBuilder> builder{nullptr};
//    std::unique_ptr<Output> output{nullptr};
//    std::unique_ptr<pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNodeBuilder>
//      pmSkiplistNvmSingleNodeBuilder{nullptr};
//  };
//
//  pmCache::Promotion::Promotion(int level) : level_(level) {}
//
//  pmCache::Promotion::~Promotion() {
//    if(inputVersion != nullptr)
//      inputVersion->Unref();
//  }
//
//  void pmCache::Promotion::AddInputDeletions(VersionEdit *edit) const {
//    edit->RemoveFile(level_ - 1, input->number);
//  }
//
//  void pmCache::Promotion::ReleaseInputs() {
//    if(inputVersion != nullptr) {
//      inputVersion->Unref();
//      inputVersion = nullptr;
//    }
//  }
//
//} // namespace leveldb