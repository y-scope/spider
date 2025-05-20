#include "CoreTaskUtils.hpp"

#include <algorithm>
#include <cmath>
#include <compare>
#include <concepts>
#include <cstddef>
#include <functional>
#include <numeric>
#include <utility>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <boost/uuid/uuid.hpp>

#include "spider/core/Task.hpp"
#include "spider/core/TaskGraph.hpp"

namespace spider::test {
namespace {
constexpr double cEpsilon = 0.0001;

auto float_equal(float f1, float f2) -> bool {
    return std::abs(f1 - f2) < cEpsilon;
}

template <class T>
auto vector_equal(
        std::vector<T> const& v1,
        std::vector<T> const& v2,
        std::function<bool(T const&, T const&)> const& equal
) -> bool {
    if (v1.size() != v2.size()) {
        return false;
    }
    for (size_t i = 0; i < v1.size(); ++i) {
        if (!equal(v1[i], v2[i])) {
            return false;
        }
    }
    return true;
}

template <class T>
requires std::totally_ordered<T>
auto vector_sort_equal(std::vector<T> const& v1, std::vector<T> const& v2) -> bool {
    if (v1.size() != v2.size()) {
        return false;
    }

    // Create new sorted indices vector instead of inplace sort
    std::vector<size_t> v1_indices(v1.size());
    std::vector<size_t> v2_indices(v2.size());
    std::iota(v1_indices.begin(), v1_indices.end(), 0);
    std::iota(v2_indices.begin(), v2_indices.end(), 0);
    std::sort(v1_indices.begin(), v1_indices.end(), [&v1](size_t i1, size_t i2) {
        return v1[i1] < v1[i2];
    });
    std::sort(v2_indices.begin(), v2_indices.end(), [&v2](size_t i1, size_t i2) {
        return v2[i1] < v2[i2];
    });

    for (int i = 0; i < v1.size(); ++i) {
        if (v1[v1_indices[i]] != v2[v2_indices[i]]) {
            return false;
        }
    }

    return true;
}

auto operator<=>(
        std::pair<boost::uuids::uuid, boost::uuids::uuid> const& p1,
        std::pair<boost::uuids::uuid, boost::uuids::uuid> const& p2
) -> std::strong_ordering {
    if (p1.first < p2.first) {
        return std::strong_ordering::less;
    }
    if (p1.first > p2.first) {
        return std::strong_ordering::greater;
    }
    if (p1.second < p2.second) {
        return std::strong_ordering::less;
    }
    if (p1.second > p2.second) {
        return std::strong_ordering::greater;
    }
    return std::strong_ordering::equal;
}

template <class K, class V, class Hash>
requires std::equality_comparable<K>
auto hash_map_equal(
        absl::flat_hash_map<K, V, Hash> const& map_1,
        absl::flat_hash_map<K, V, Hash> const& map_2,
        std::function<bool(V const&, V const&)> const& value_equal
) -> bool {
    if (map_1.size() != map_2.size()) {
        return false;
    }

    if (std::ranges::any_of(map_1, [&map_2, &value_equal](auto const& pair_1) -> bool {
            if (auto const& iter_2 = map_2.find(pair_1.first); iter_2 != map_2.cend()) {
                return !value_equal(pair_1.second, iter_2->second);
            }
            return true;
        }))
    {
        return false;
    }

    return true;
}
}  // namespace

auto task_graph_equal(core::TaskGraph const& graph_1, core::TaskGraph const& graph_2) -> bool {
    if (!hash_map_equal<boost::uuids::uuid, core::Task, std::hash<boost::uuids::uuid>>(
                graph_1.get_tasks(),
                graph_2.get_tasks(),
                task_equal
        ))
    {
        return false;
    }
    if (!vector_sort_equal<std::pair<boost::uuids::uuid, boost::uuids::uuid>>(
                graph_1.get_dependencies(),
                graph_2.get_dependencies()
        ))
    {
        return false;
    }
    return true;
}

auto task_equal(core::Task const& t1, core::Task const& t2) -> bool {
    if (t1.get_id() != t2.get_id()) {
        return false;
    }
    if (t1.get_function_name() != t2.get_function_name()) {
        return false;
    }
    // Task state might not be the same
    // if (t1.get_state() != t2.get_state()) {
    //     return false;
    // }
    if (!float_equal(t1.get_timeout(), t2.get_timeout())) {
        return false;
    }
    if (!vector_equal<core::TaskInput>(t1.get_inputs(), t2.get_inputs(), task_input_equal)) {
        return false;
    }
    if (!vector_equal<core::TaskOutput>(t1.get_outputs(), t2.get_outputs(), task_output_equal)) {
        return false;
    }
    return true;
}

auto task_input_equal(core::TaskInput const& input_1, core::TaskInput const& input_2) -> bool {
    if (input_1.get_task_output() != input_2.get_task_output()) {
        return false;
    }
    if (input_1.get_value() != input_2.get_value()) {
        return false;
    }
    if (input_1.get_data_id() != input_2.get_data_id()) {
        return false;
    }
    if (input_1.get_type() != input_2.get_type()) {
        return false;
    }
    return true;
}

auto task_output_equal(core::TaskOutput const& output_1, core::TaskOutput const& output_2) -> bool {
    if (output_1.get_data_id() != output_2.get_data_id()) {
        return false;
    }
    if (output_1.get_value() != output_2.get_value()) {
        return false;
    }
    if (output_1.get_type() != output_2.get_type()) {
        return false;
    }
    return true;
}
}  // namespace spider::test
