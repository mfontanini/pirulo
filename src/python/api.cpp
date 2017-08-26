#include <boost/optional.hpp>
#include <boost/python/module.hpp>
#include <boost/python/class.hpp>
#include <boost/python/object.hpp>
#include <boost/python/str.hpp>
#include <boost/python/import.hpp>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>
#include "python/api.h"
#include "python/helpers.h"
#include "python/handler.h"
#include "detail/logging.h"

using std::string;
using std::vector;
using std::shared_ptr;

using boost::optional;

namespace python = boost::python;

namespace pirulo {

PIRULO_CREATE_LOGGER("p.python");

BOOST_PYTHON_MODULE(pirulo) {
    python::class_<Handler>("Handler")
        .def("initialize", &Handler::initialize)
        .def("subscribe_to_consumers", &Handler::subscribe_to_consumers)
        .def("subscribe_to_consumer_commits", &Handler::subscribe_to_consumer_commits)
        .def("subscribe_to_topics", &Handler::subscribe_to_topics)
        .def("subscribe_to_topic_message", &Handler::subscribe_to_topic_message)
    ;
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

void register_module() {
    PyImport_AppendInittab("pirulo", &initpirulo);
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

    class_<OffsetStore, shared_ptr<OffsetStore>, boost::noncopyable>("OffsetStore", no_init)
        .def("get_consumers", &OffsetStore::get_consumers)
        .def("get_consumer_offsets", &OffsetStore::get_consumer_offsets)
        .def("get_topic_offset", &OffsetStore::get_topic_offset)
        .def("get_topics", &OffsetStore::get_topics)
        .def("on_new_consumer", +[](OffsetStore& store, const object& callback) {
            store.on_new_consumer([=](const string& group_id) {
                helpers::safe_exec(logger, [&]() {
                    call<void>(callback.ptr(), group_id);
                });
            });
        })
        .def("on_new_topic", +[](OffsetStore& store, const object& callback) {
            store.on_new_topic([=](const string& topic) {
                helpers::safe_exec(logger, [&]() {
                    call<void>(callback.ptr(), topic);
                });
            });
        })
        .def("on_consumer_commit", +[](OffsetStore& store, const string& group_id,
                                       const object& callback) {
            store.on_consumer_commit(group_id, [=](const string& group_id, const string& topic,
                                                   int partition, uint64_t offset) {
                helpers::safe_exec(logger, [&]() {
                    call<void>(callback.ptr(), group_id, topic, partition, offset);
                });
            });
        })
        .def("on_topic_message", +[](OffsetStore& store, const string& topic,
                                     const object& callback) {
            store.on_topic_message(topic, [=](const string& topic, int partition,
                                              uint64_t offset) {
                helpers::safe_exec(logger, [&]() {
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

} // pirulo
