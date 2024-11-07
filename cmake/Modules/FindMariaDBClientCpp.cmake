# Try to find MariaDBClientCpp
#
# Set MariaDBClientCpp_USE_STATIC_LIBS=ON to look for static libraries.
#
# Once done this will define:
#  MariaDBClientCpp_FOUND - Whether MariaDBClient was found on the system
#  MariaDBClientCpp_INCLUDE_DIR - The MariaDBClient include directories
#  MariaDBClientCpp_VERSION - The version of MariaDBClient installed on the system
#
# Conventions:
# - Variables only for use within the script are prefixed with "mariadbclientcpp_"
# - Variables that should be externally visible are prefixed with "MariaDBClientCpp_"

set(mariadbclientcpp_LIBNAME "mariadbcpp")

include(cmake/Modules/FindLibraryDependencies.cmake)

# Run pkg-config
find_package(PkgConfig)
pkg_check_modules(mariadbclientcpp_PKGCONF QUIET "lib${mariadbclientcpp_LIBNAME}")

# Set include directory
find_path(
    MariaDBClientCpp_INCLUDE_DIR
    conncpp.hpp
    HINTS
        ${mariadbclientcpp_PKGCONF_INCLUDEDIR}
    PATH_SUFFIXES
        mariadb
)

# Handle static libraries
if(MariaDBClientCpp_USE_STATIC_LIBS)
    # Save current value of CMAKE_FIND_LIBRARY_SUFFIXES
    set(mariadbclientcpp_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})

    # Temporarily change CMAKE_FIND_LIBRARY_SUFFIXES to static library suffix
    set(CMAKE_FIND_LIBRARY_SUFFIXES .a)
else()
    # mariadb-connector-cpp uses .dylib for dynamic library, at least on macOS
    set(mariadbclientcpp_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
    set(CMAKE_FIND_LIBRARY_SUFFIXES
        .so
        .dylib
    )
endif()

# Find library
find_library(
    MariaDBClientCpp_LIBRARY
    NAMES
        ${mariadbclientcpp_LIBNAME}
    HINTS
        ${mariadbclientcpp_PKGCONF_LIBDIR}
    PATH_SUFFIXES
        mariadb
)
if(MariaDBClientCpp_LIBRARY)
    # NOTE: This must be set for find_package_handle_standard_args to work
    set(MariaDBClientCpp_FOUND ON)
endif()

if(MariaDBClientCpp_USE_STATIC_LIBS)
    findstaticlibrarydependencies(${mariadbclientcpp_LIBNAME} mariadbclientcpp
        "${mariadbclientcpp_PKGCONF_STATIC_LIBRARIES}"
    )

    # Restore original value of CMAKE_FIND_LIBRARY_SUFFIXES
    set(CMAKE_FIND_LIBRARY_SUFFIXES ${mariadbclientcpp_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES})
    unset(mariadbclientcpp_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES)
else()
    set(CMAKE_FIND_LIBRARY_SUFFIXES ${mariadbclientcpp_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES})
    unset(mariadbclientcpp_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES)
endif()

finddynamiclibrarydependencies(mariadbclientcpp "${mariadbclientcpp_DYNAMIC_LIBS}")

# Set version
set(MariaDBClientCpp_VERSION ${mariadbclientcpp_PKGCONF_VERSION})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
    MariaDBClientCpp
    REQUIRED_VARS
        MariaDBClientCpp_INCLUDE_DIR
    VERSION_VAR MariaDBClientCpp_VERSION
)

if(NOT TARGET MariaDBClientCpp::MariaDBClientCpp)
    # Add library to build
    if(MariaDBClientCpp_FOUND)
        if(MariaDBClientCpp_USE_STATIC_LIBS)
            add_library(MariaDBClientCpp::MariaDBClientCpp STATIC IMPORTED GLOBAL)
        else()
            # NOTE: We use UNKNOWN so that if the user doesn't have the SHARED
            # libraries installed, we can still use the STATIC libraries
            add_library(MariaDBClientCpp::MariaDBClientCpp UNKNOWN IMPORTED GLOBAL)
        endif()
    endif()

    # Set include directories for library
    if(MariaDBClientCpp_INCLUDE_DIR)
        set_target_properties(
            MariaDBClientCpp::MariaDBClientCpp
            PROPERTIES
                INTERFACE_INCLUDE_DIRECTORIES
                    "${MariaDBClientCpp_INCLUDE_DIR};${MariaDBClientCpp_INCLUDE_DIR}/conncpp;${MariaDBClientCpp_INCLUDE_DIR}/conncpp/compat"
        )
    endif()

    # Set location of library
    if(EXISTS "${MariaDBClientCpp_LIBRARY}")
        set_target_properties(
            MariaDBClientCpp::MariaDBClientCpp
            PROPERTIES
                IMPORTED_LINK_INTERFACE_LANGUAGES
                    "CXX"
                IMPORTED_LOCATION
                    "${MariaDBClientCpp_LIBRARY}"
        )

        # Add component's dependencies for linking
        if(mariadbclientcpp_LIBRARY_DEPENDENCIES)
            set_target_properties(
                MariaDBClientCpp::MariaDBClientCpp
                PROPERTIES
                    INTERFACE_LINK_LIBRARIES
                        "${mariadbclientcpp_LIBRARY_DEPENDENCIES}"
            )
        endif()
    endif()
endif()
