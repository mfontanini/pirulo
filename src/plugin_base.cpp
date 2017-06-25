#include "plugin_base.h"

using std::move;

namespace pirulo {

void PluginBase::launch(StorePtr store) {
    store_ = move(store);
    initialize();
}

PluginBase::StorePtr PluginBase::get_store() const {
    return store_;
}

} // pirulo
