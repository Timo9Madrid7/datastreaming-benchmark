add_executable(LoggerTest LoggerTest.cpp)
target_link_libraries(LoggerTest PRIVATE core-logger)

add_executable(PayloadTest PayloadTest.cpp)
target_link_libraries(PayloadTest PRIVATE core-payload)