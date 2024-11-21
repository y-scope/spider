#ifndef SPIDER_CLIENT_DATA_HPP
#define SPIDER_CLIENT_DATA_HPP

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "../core/Serializer.hpp"

namespace spider {
class DataImpl;

/**
 * Data represents metadata of data stored on external storage. Data provides hint for Spider of
 * metadata information like locality of the data to improve scheduling decision.
 * Example:
 *     spider::Data<std::string> disk_file_data = spider::Data<std::string>::Builder()
 *         .set_locality("node_address", true)
 *         .set_cleanup([](std::string cont& path) { std::remove(path); })
 *         .build("/path/of/file");
 *
 * Data is passed in as input so the tasks can get the value of the data.
 *
 * @tparam T type of the value.
 */
template <Serializable T>
class Data {
public:
    /**
     * Gets the values stored in Data.
     * @return The stored value.
     */
    auto get() -> T;

    /**
     * Sets locality list of the data.
     *
     * @param nodes
     * @param hard true if the locality list is a hard requirement, false otherwise. Hard locality
     * requirement means that data can only be accessed from `nodes`.
     */
    void set_locality(std::vector<std::string> const& nodes, bool hard);

    class Builder {
    public:
        /**
         * Sets locality list of the data to build.
         *
         * @param nodes
         * @param hard true if the locality list is a hard requirement, false otherwise
         * @return self
         */
        auto set_locality(std::vector<std::string> const& nodes, bool hard) -> Data<T>::Builder&;

        /**
         * Defines clean up function of the data to build.
         *
         * @param f
         * @return self
         */
        auto set_cleanup(std::function<T const&()> const& f) -> Data<T>::Builder&;

        /**
         * Builds the data. Stores the value of data into storage with locality list and cleanup
         * functions.
         *
         * @param t Value of the data
         * @return Data object created.
         */
        auto build(T const& t) -> Data<T>;
    };

private:
    std::unique_ptr<DataImpl> m_impl;
};

}  // namespace spider

#endif  // SPIDER_CLIENT_DATA_HPP
