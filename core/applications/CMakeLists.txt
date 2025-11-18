add_executable(ConsumerApp ConsumerApp.cpp)
target_link_libraries(ConsumerApp PUBLIC core-loader core-interfaces factories)


add_executable(PublisherApp PublisherApp.cpp)
target_link_libraries(PublisherApp PUBLIC core-loader core-interfaces factories)