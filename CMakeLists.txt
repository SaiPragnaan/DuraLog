cmake_minimum_required(VERSION 3.20)
project(DuraLog LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(duralog_lib src/wal.cpp)
target_include_directories(duralog_lib PUBLIC include)
target_compile_options(duralog_lib PRIVATE -Wall -Wextra -Wpedantic -Werror)

add_executable(duralog src/main.cpp)
target_link_libraries(duralog PRIVATE duralog_lib)
target_compile_options(duralog PRIVATE -Wall -Wextra -Wpedantic -Werror)

add_executable(duralog_tests tests/wal_tests.cpp)
target_link_libraries(duralog_tests PRIVATE duralog_lib)
target_compile_options(duralog_tests PRIVATE -Wall -Wextra -Wpedantic -Werror)

enable_testing()
add_test(NAME duralog_tests COMMAND duralog_tests)
