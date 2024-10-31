#ifndef SPIDER_CLIENT_DATA_HPP
#define SPIDER_CLIENT_DATA_HPP

#include <functional>
#include <memory>

namespace spider {

class DataImpl;

template <class T>
class Data {
private:
    std::unique_ptr<DataImpl> m_impl;

public:
    /**
     * Gets the values stored in Data.
     * @return value stored in Data.
     */
    auto get() -> T;
    /**
     * Indicates that the data is persisted and should not be rollbacked
     * on failure recovery.
     */
    // Not implemented in milestone 1
    // void mark_persist();
    /**
     * Sets locality list of the data.
     * @param nodes nodes that has locality
     * @param hard true if the locality list is a hard requirement, false otherwise
     */
    void set_locality(std::vector<std::string> const& nodes, bool hard);

    class Builder {
    private:
    public:
        /**
         * Sets the key for the data. If no key is provided, Spider generates a key.
         * @param key of the data
         */
        auto key(std::string const& key) -> Data<T>::Builder&;
        /**
         * Sets locality list of the data to build.
         * @param nodes nodes that has locality
         * @param hard true if the locality list is a hard requirement, false otherwise
         * @return self
         */
        auto locality(std::vector<std::string> const& nodes, bool hard) -> Data<T>::Builder&;
        /**
         * Indicates that the data to build is persisted and should not be rollbacked on failure
         * recovery.
         * @return self
         */
        // Data<T>::Builder Builder& mark_persist(); // Not implemented in milestone 1
        /**
         * Defines clean up functions of the data to build.
         * @param f clean up function of data
         */
        auto cleanup(std::function<T const&()> const& f) -> Data<T>::Builder&;
        /**
         * Defines rollback functions of the data to build.
         * @param f rollback function of data
         */
        // Not implemented for milestone 1
        // auto rollback(std::function<const T&()> const& f) -> Data<T>::Builder&;
        /**
         * Builds the data. Stores the value of data into storage with locality list, persisted
         * flag, cleanup and rollback functions.
         * @param t value of the data
         * @return data object
         */
        auto build(T&& t) -> Data<T>;
    };
};

}  // namespace spider

#endif  // SPIDER_CLIENT_DATA_HPP
