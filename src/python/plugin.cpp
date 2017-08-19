#include <stdexcept>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>
#include "python/plugin.h"
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

template <typename T>
struct value_or_none {
    static PyObject* convert(const optional<T>& value) {
        if (value) {
            return python::incref(python::object(*value).ptr());
        }
        else {
            return Py_None;
        }
    }
};

// Taken from https://stackoverflow.com/questions/1418015/how-to-get-python-exception-text
string handle_pyerror() {
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

template <typename Functor>
void safe_exec(const Functor& python_code) {
    try {
        python_code();
    }
    catch (const python::error_already_set& ex) {
        LOG4CXX_ERROR(logger, "Error executing python callback: " << handle_pyerror());
    }
}

void register_types() {
    using python::to_python_converter;
    using python::class_;
    using python::no_init;
    using python::make_function;
    using python::return_internal_reference;
    using python::vector_indexing_suite;
    using python::object;
    using python::call;

    to_python_converter<optional<int64_t>, value_or_none<int64_t>>();

    class_<ConsumerOffset>("ConsumerOffset", no_init)
        .add_property("group_id",
                      make_function(&ConsumerOffset::get_group_id,
                                    return_internal_reference<>()))
        .add_property("topic", +[](const ConsumerOffset& o) {
            return o.get_topic_partition().get_topic();
        })
        .add_property("partition", +[](const ConsumerOffset& o) {
            return o.get_topic_partition().get_partition();
        })
        .add_property("offset", +[](const ConsumerOffset& o) {
            return o.get_topic_partition().get_offset();
        })
        ;

    class_<OffsetStore, boost::noncopyable>("OffsetStore", no_init)
        .def("get_consumers", &OffsetStore::get_consumers)
        .def("get_consumer_offsets", &OffsetStore::get_consumer_offsets)
        .def("get_topic_offset", &OffsetStore::get_topic_offset)
        .def("on_new_consumer", +[](OffsetStore& store, const object& callback) {
            store.on_new_consumer([=](const string& group_id) {
                safe_exec([&]() {
                    call<void>(callback.ptr(), group_id);
                });
            });
        })
        .def("on_consumer_commit", +[](OffsetStore& store, const string& group_id,
                                       const object& callback) {
            store.on_consumer_commit(group_id, [=](const string& group_id, const string& topic,
                                                   int partition, uint64_t offset) {
                safe_exec([&]() {
                    call<void>(callback.ptr(), group_id, topic, partition, offset);
                });
            });
        })
        ;

    class_<vector<string>>("StringVector")
        .def(vector_indexing_suite<vector<string>>())
        ;

    class_<vector<ConsumerOffset>>("ConsumerOffsetVector")
        .def(vector_indexing_suite<vector<ConsumerOffset>>())
        ;
}

void initialize_python() {
    static once_flag flag;
    call_once(flag, [&] {
        Py_Initialize();
        register_types();
    });
}

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
