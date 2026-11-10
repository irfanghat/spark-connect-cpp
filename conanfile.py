
from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMake, cmake_layout, CMakeDeps
from conan.tools.gnu import PkgConfigDeps


class SparkConnectCpp(ConanFile):
    name = "spark_connect_cpp"
    version = "0.1.0"
    package_type = "static-library"

    license = "Apache-2.0"
    author = "Irfan Ghat <irfanghat@gmail.com>"
    url = "https://github.com/irfanghat/spark-connect-cpp"
    description = (
        "Spark Connect C++. Making Apache Spark easily "
        "accessible to C++ developers."
    )
    topics = ("apache-spark", "spark", "big-data", "cpp")

    settings = "os", "compiler", "build_type", "arch"

    exports_sources = (
        "CMakeLists.txt",
        "src/**",
        "include/**",
        "cmake/**",
        "spark_connect_cpp.pc.in",
        "LICENSE",
    )

    def requirements(self):
        self.requires("grpc/1.84.0")
        self.requires("protobuf/6.33.5")
        self.requires("arrow/25.0.1")
        self.requires("libuuid/1.0.3")

    def layout(self):
        cmake_layout(self)

    def generate(self):
        deps = CMakeDeps(self)
        deps.generate()

        pkg_config = PkgConfigDeps(self)
        pkg_config.generate()

        tc = CMakeToolchain(self)
        tc.variables["SPARK_CONNECT_CPP_BUILD_TESTS"] = False
        tc.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        cmake = CMake(self)
        cmake.install()

    def package_info(self):
        self.cpp_info.set_property(
            "cmake_file_name", "spark_connect_cpp"
        )

        # ------------------------------------
        # NOTE
        # Main library
        # ------------------------------------
        main = self.cpp_info.components["spark_connect_cpp"]
        main.libs = ["spark_connect_cpp"]
        main.includedirs = ["include/spark_connect_cpp"]
        main.set_property(
            "cmake_target_name", "spark_connect_cpp::spark_connect_cpp"
        )
        main.requires = ["spark_connect"]

        # ------------------------------------
        # NOTE
        # Generated protobuf/gRPC library
        # ------------------------------------
        proto = self.cpp_info.components["spark_connect"]
        proto.libs = ["spark_connect"]
        proto.includedirs = ["include/spark_connect_cpp"]
        proto.set_property(
            "cmake_target_name", "spark_connect_cpp::spark_connect"
        )
