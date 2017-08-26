#include <stdexcept>
#include <boost/python/object.hpp>
#include <boost/python/import.hpp>
#include <boost/python/exec.hpp>
#include <boost/python/dict.hpp>
#include "python/plugin.h"
#include "python/helpers.h"
#include "detail/logging.h"
#include "offset_store.h"

using std::string;
using std::vector;
using std::once_flag;
using std::call_once;
using std::runtime_error;

using boost::optional;

namespace python = boost::python;

namespace pirulo {

PIRULO_CREATE_LOGGER("p.python");

PythonPlugin::PythonPlugin(const string& modules_path, const string& file_path) {
    helpers::initialize_python();
    helpers::GILAcquirer _;

    // Dirtiness taken from https://wiki.python.org/moin/boost.python/EmbeddingPython
    python::dict locals;

    python::object main_module = python::import("__main__");
    python::object main_namespace = main_module.attr("__dict__");

    locals["modules_path"] = modules_path;
    locals["path"] = file_path;
    try {
        python::exec("import imp, sys, os.path\n"
             "sys.path.append(os.path.abspath(modules_path))\n"
             "module = imp.load_module('plugin',open(path),path,('py','U',imp.PY_SOURCE))\n",
             main_namespace, locals);
    }
    catch (const python::error_already_set& ex) {
        LOG4CXX_ERROR(logger, "Failed to load module " << file_path
                      << ": " << helpers::format_python_exception());
        throw runtime_error("Error loading python plugin " + file_path);
    }

    try {
        python::object plugin_class = locals["module"].attr("Plugin");
        plugin_ = plugin_class();
    }
    catch (const python::error_already_set& ex) {
        LOG4CXX_ERROR(logger, "Error instantiating Plugin classs on module " << file_path
                      << ": " << helpers::format_python_exception());
        throw runtime_error("Error instanting python plugin " + file_path);
    }
}

void PythonPlugin::initialize() {
    helpers::GILAcquirer _;
    try {
        const StorePtr& store = get_store();
        plugin_.attr("initialize")(store);
    }
    catch (const python::error_already_set& ex) {
        LOG4CXX_ERROR(logger, "Error initializing plugin: "
                      << helpers::format_python_exception());
    }
}

} // pirulo
