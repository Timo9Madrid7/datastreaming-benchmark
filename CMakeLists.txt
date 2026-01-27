cmake_minimum_required(VERSION 3.16)
project(MessagingBenchmark)

# message(STATUS "ROOT: Toolchain file: ${CMAKE_TOOLCHAIN_FILE}")

# # Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Platform-specific setup
if (WIN32)
    message(STATUS "ROOT: Building on Windows")
    # vcpkg integrates automatically with CMake if the toolchain file is set
    set(CMAKE_TOOLCHAIN_FILE "../vcpkg/scripts/buildsystems/vcpkg.cmake" CACHE STRING "")
elseif (UNIX)
    message(STATUS "ROOT: Building on Linux")
endif()

# Add subdirectories
add_subdirectory(core)
add_subdirectory(core/applications)
add_subdirectory(core/tests)

# Maybe wrap these repeated if-blocks in a function/macro or table later
foreach(TECH nats_p2p arrowflight_p2p arrowflight_bin_p2p kafka_p2p zeromq_p2p rabbitmq_p2p)
    if(EXISTS "${CMAKE_SOURCE_DIR}/technologies/${TECH}")
        message(STATUS "Including ${TECH} technology")
        add_subdirectory("technologies/${TECH}")
    else()
        message(STATUS "Skipping ${TECH} technology (not found)")
    endif()
endforeach()




# target_link_libraries(MessagingBenchmark PRIVATE core)