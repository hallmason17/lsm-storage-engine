#include "Wal.h"
namespace lsm_storage_engine {
void Wal::write(const std::string &message) { file_ << message; }
} // namespace lsm_storage_engine
