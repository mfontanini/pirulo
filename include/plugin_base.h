#pragma once

#include <memory>
#include "offset_store.h"

namespace pirulo {

class PluginBase {
public:
    using StorePtr = std::shared_ptr<OffsetStore>;

    virtual ~PluginBase() = default;

    void launch(StorePtr store);
protected:
    StorePtr get_store() const;
private:
    virtual void initialize() = 0;

    StorePtr store_;
};

} // pirulo
