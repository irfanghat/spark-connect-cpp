cmake_minimum_required(VERSION 3.25.1)


project(SparkConnectCpp CXX)


add_executable(my_app main.cpp)


target_compile_features(spark_connect_cpp PRIVATE cxx_std_17)

