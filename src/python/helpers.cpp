#include <atomic>
#include <boost/python/object.hpp>
#include <boost/python/str.hpp>
#include <boost/python/import.hpp>
#include <boost/python/extract.hpp>
#include "python/helpers.h"
#include "python/api.h"
#include "offset_store.h"
#include "detail/logging.h"

using std::string;
using std::vector;
using std::call_once;
using std::once_flag;

namespace python = boost::python;

namespace pirulo {
namespace helpers {

PIRULO_CREATE_LOGGER("p.python");

GILAcquirer::GILAcquirer()
: gstate_(PyGILState_Ensure()) {
}

GILAcquirer::~GILAcquirer() {
    PyGILState_Release(gstate_);
}

// Taken from https://stackoverflow.com/questions/1418015/how-to-get-python-exception-text
string format_python_exception() {
    using namespace boost::python;
    using namespace boost;

    PyObject *exc, *val, *tb;
    object formatted_list, formatted;
    PyErr_Fetch(&exc, &val, &tb);
    handle<> hexc(exc);
    handle<> hval(allow_null(val));
    handle<> htb(allow_null(tb)); 
    object traceback(import("traceback"));
    if (!tb) {
        object format_exception_only(traceback.attr("format_exception_only"));
        formatted_list = format_exception_only(hexc, hval);
    } else {
        object format_exception(traceback.attr("format_exception"));
        formatted_list = format_exception(hexc, hval, htb);
    }
    formatted = str("\n").join(formatted_list);
    return extract<string>(formatted);
}

void initialize_python() {
    static once_flag flag;
    call_once(flag, [&] {
        register_module();
        PyEval_InitThreads();
        Py_Initialize();
        register_types();
        PyEval_SaveThread();
    });
}

} // helpers
} // pirulo
