/* Copyright 2017-2023 Institute for Automation of Complex Power Systems,
 *                     EONERC, RWTH Aachen University
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *********************************************************************************/

#pragma once

#ifdef USE_GHC_FS
#include <ghc/filesystem.hpp>
namespace fs = ghc::filesystem;
#elif defined(__has_include)
#if __has_include(<filesystem>)
#include <filesystem>
namespace fs = std::filesystem;
#elif __has_include(<experimental/filesystem>)
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#else
#error "No <filesystem> or <experimental/filesystem> header found"
#endif
#else
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif

#include <spdlog/fmt/ostr.h>

#if FMT_VERSION >= 90000
template <> class fmt::formatter<fs::path> : public fmt::ostream_formatter {};
#endif
