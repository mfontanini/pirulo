#include <stdexcept>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>
#include "python/plugin.h"
#include "python/interoperability.h"
#include "detail/logging.h"
#include "offset_store.h"

using std::string;
using std::vector;
using std::once_flag;
using std::call_once;
using std::runtime_error;

namespace python = boost::python;

namespace pirulo {

void initialize_python() {
    static once_flag flag;
    call_once(flag, [&] {
        Py_Initialize();

        python::class_<OffsetStore, boost::noncopyable>("OffsetStore", python::no_init)
            .def("get_consumers", &OffsetStore::get_consumers);


        python::class_<vector<string> >("StringVector")
            .def(python::vector_indexing_suite<vector<string>>());
    });
}

// Taken from https://stackoverflow.com/questions/1418015/how-to-get-python-exception-text
string handle_pyerror()
{
    using namespace boost::python;
    using namespace boost;

    PyObject *exc, *val, *tb;
    object formatted_list, formatted;
    PyErr_Fetch(&exc,&val,&tb);
    handle<> hexc(exc),hval(allow_null(val)),htb(allow_null(tb)); 
    object traceback(import("traceback"));
    if (!tb) {
        object format_exception_only(traceback.attr("format_exception_only"));
        formatted_list = format_exception_only(hexc,hval);
    } else {
        object format_exception(traceback.attr("format_exception"));
        formatted_list = format_exception(hexc,hval,htb);
    }
    formatted = str("\n").join(formatted_list);
    return extract<string>(formatted);
}

PIRULO_CREATE_LOGGER("p.python");

PythonPlugin::PythonPlugin(const string& file_path) {
    initialize_python();
    // Dirtiness taken from https://wiki.python.org/moin/boost.python/EmbeddingPython
    python::dict locals;

    python::object main_module = python::import("__main__");
    python::object main_namespace = main_module.attr("__dict__");

    locals["path"] = file_path;
    try {
        python::exec("import imp\n"
             "module = imp.load_module('plugin',open(path),path,('py','U',imp.PY_SOURCE))\n",
             main_namespace, locals);
    }
    catch (const python::error_already_set& ex) {
        LOG4CXX_ERROR(logger, "Failed to load module " << file_path
                      << ": " << handle_pyerror());
        throw runtime_error("Error loading python plugin " + file_path);
    }

    try {
        python::object plugin_class = locals["module"].attr("Plugin");
        plugin_ = plugin_class();
    }
    catch (const python::error_already_set& ex) {
        LOG4CXX_ERROR(logger, "Error instantiating Plugin classs on module " << file_path
                      << ": " << handle_pyerror());
        throw runtime_error("Error instanting python plugin " + file_path);
    }
}

void PythonPlugin::initialize() {
    try {
        StorePtr store = get_store();
        plugin_.attr("initialize")(python::ptr(store.get()));
    }
    catch (const python::error_already_set& ex) {
        LOG4CXX_ERROR(logger, "Error initializing plugin: " << handle_pyerror());
    }
}

} // pirulo
