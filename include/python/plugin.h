#pragma once

#include <string>
#include <boost/python.hpp>
#include "plugin_base.h"

namespace pirulo {

class PythonPlugin : public PluginBase {
public:
    PythonPlugin(const std::string& file_path);
private:
    void initialize();

    boost::python::object plugin_;
};

} // pirulo
