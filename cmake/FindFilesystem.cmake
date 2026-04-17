# Detect how to link against std::filesystem.
#
# Apple Clang (libc++) and modern libstdc++ / libc++ provide std::filesystem
# under C++17 with no extra link library. GCC < 9 requires -lstdc++fs, and
# Clang < 9 on libc++ requires -lc++fs. Rather than hard-coding a library
# per compiler, probe with a test compile and fall back only when needed.

include(CMakePushCheckState)
include(CheckCXXSourceCompiles)

cmake_push_check_state(RESET)
set(CMAKE_REQUIRED_QUIET TRUE)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_CXX_STANDARD OR CMAKE_CXX_STANDARD LESS 17)
	set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} -std=c++17")
endif()

set(_filesystem_test_src [=[
#if defined(__has_include)
#  if __has_include(<filesystem>)
#    include <filesystem>
     namespace fs = std::filesystem;
#  elif __has_include(<experimental/filesystem>)
#    include <experimental/filesystem>
     namespace fs = std::experimental::filesystem;
#  else
#    error "no <filesystem> or <experimental/filesystem>"
#  endif
#else
#  include <experimental/filesystem>
   namespace fs = std::experimental::filesystem;
#endif
int main() {
    fs::path p("/");
    return fs::exists(p) ? 0 : 1;
}
]=])

# 1) No extra link library (Apple Clang / libc++, modern libstdc++).
check_cxx_source_compiles("${_filesystem_test_src}" FILESYSTEM_NO_LINK_NEEDED)

set(FILESYSTEM_LIBRARY "")

if(NOT FILESYSTEM_NO_LINK_NEEDED)
	# 2) GCC < 9 / older libstdc++: needs -lstdc++fs.
	set(CMAKE_REQUIRED_LIBRARIES stdc++fs)
	check_cxx_source_compiles("${_filesystem_test_src}" FILESYSTEM_NEEDS_STDCXXFS)
	if(FILESYSTEM_NEEDS_STDCXXFS)
		set(FILESYSTEM_LIBRARY stdc++fs)
	else()
		# 3) Clang < 9 on libc++: needs -lc++fs.
		set(CMAKE_REQUIRED_LIBRARIES c++fs)
		check_cxx_source_compiles("${_filesystem_test_src}" FILESYSTEM_NEEDS_CXXFS)
		if(FILESYSTEM_NEEDS_CXXFS)
			set(FILESYSTEM_LIBRARY c++fs)
		endif()
	endif()
endif()

cmake_pop_check_state()

if(FILESYSTEM_NO_LINK_NEEDED OR FILESYSTEM_NEEDS_STDCXXFS OR FILESYSTEM_NEEDS_CXXFS)
	set(FILESYSTEM_TEST_PASSED TRUE)
else()
	set(FILESYSTEM_TEST_PASSED FALSE)
endif()

set(FILESYSTEM_LIBRARIES ${FILESYSTEM_LIBRARY})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Filesystem REQUIRED_VARS FILESYSTEM_TEST_PASSED)

mark_as_advanced(FILESYSTEM_LIBRARY FILESYSTEM_LIBRARIES)

add_library(filesystem INTERFACE)
if(FILESYSTEM_LIBRARY)
	target_link_libraries(filesystem INTERFACE ${FILESYSTEM_LIBRARY})
endif()
