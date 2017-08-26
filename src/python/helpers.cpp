#include <atomic>
#include <boost/optional.hpp>
#include <boost/python/object.hpp>
#include <boost/python/str.hpp>
#include <boost/python/import.hpp>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>
#include "python/helpers.h"
#include "offset_store.h"
#include "detail/logging.h"

using std::string;
using std::vector;
using std::call_once;
using std::once_flag;

using boost::optional;

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
        .def("get_topics", &OffsetStore::get_topics)
        .def("on_new_consumer", +[](OffsetStore& store, const object& callback) {
            store.on_new_consumer([=](const string& group_id) {
                safe_exec(logger, [&]() {
                    call<void>(callback.ptr(), group_id);
                });
            });
        })
        .def("on_new_topic", +[](OffsetStore& store, const object& callback) {
            store.on_new_topic([=](const string& topic) {
                safe_exec(logger, [&]() {
                    call<void>(callback.ptr(), topic);
                });
            });
        })
        .def("on_consumer_commit", +[](OffsetStore& store, const string& group_id,
                                       const object& callback) {
            store.on_consumer_commit(group_id, [=](const string& group_id, const string& topic,
                                                   int partition, uint64_t offset) {
                safe_exec(logger, [&]() {
                    call<void>(callback.ptr(), group_id, topic, partition, offset);
                });
            });
        })
        .def("on_topic_message", +[](OffsetStore& store, const string& topic,
                                     const object& callback) {
            store.on_topic_message(topic, [=](const string& topic, int partition,
                                              uint64_t offset) {
                safe_exec(logger, [&]() {
                    call<void>(callback.ptr(), topic, partition, offset);
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
        PyEval_InitThreads();
        Py_Initialize();
        register_types();
        PyEval_SaveThread();
    });
}

} // helpers
} // pirulo
