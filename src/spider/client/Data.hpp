#ifndef SPIDER_CLIENT_DATA_HPP
#define SPIDER_CLIENT_DATA_HPP

#include <functional>
#include <memory>
#include <string>
#include <vector>

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
 * Data could also be used as a key-value store.
 * Example:
 *     spider::Data<std::string> key_value_data = spider::Data<std::string>::Builder()
 *         .set_key("key")
 *         .build("value");
 *
 * @tparam T type of the value. T must be a POD.
 */
template <class T>
class Data {
public:
    /**
     * Gets the values stored in Data.
     * @return The stored value.
     */
    auto get() -> T;

    /**
     * Sets locality list of the data.
     * @param nodes nodes that has locality
     * @param hard true if the locality list is a hard requirement, false otherwise. Hard locality
     * requirement means that data can only be accessed from the node in the locality list.
     */
    void set_locality(std::vector<std::string> const& nodes, bool hard);

    class Builder {
    public:
        /**
         * Sets the key for the data.
         * @param key of the data
         * @return self
         */
        auto set_key(std::string const& key) -> Data<T>::Builder&;
        /**
         * Sets locality list of the data to build.
         * @param nodes nodes that has locality
         * @param hard true if the locality list is a hard requirement, false otherwise
         * @return self
         */
        auto set_locality(std::vector<std::string> const& nodes, bool hard) -> Data<T>::Builder&;

        /**
         * Defines clean up functions of the data to build.
         * @param f clean up function of data
         */
        auto set_cleanup(std::function<T const&()> const& f) -> Data<T>::Builder&;

        /**
         * Builds the data. Stores the value of data into storage with locality list and cleanup
         * functions.
         * @param t value of the data
         * @return data object
         */
        auto build(T const& /*t*/) -> Data<T>;
    };

private:
    std::unique_ptr<DataImpl> m_impl;
};

}  // namespace spider

#endif  // SPIDER_CLIENT_DATA_HPP
