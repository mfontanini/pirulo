#pragma once

#include <string>
#include <boost/python/object.hpp>
#include "plugin_base.h"

namespace pirulo {
namespace api {

class PythonPlugin : public PluginBase {
public:
    PythonPlugin(const std::string& modules_path, const std::string& file_path);
    ~PythonPlugin();
private:
    void initialize();

    boost::python::object plugin_;
};

} // api
} // pirulo
